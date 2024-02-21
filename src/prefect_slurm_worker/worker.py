import asyncio
import subprocess
import sys
from datetime import timedelta
from enum import IntEnum
from pathlib import Path

import anyio.abc
from prefect._internal.pydantic import HAS_PYDANTIC_V2
from prefect.client.schemas import FlowRun
from prefect.logging.loggers import PrefectLogAdapter
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.utilities.filesystem import relative_path_to_current_platform
from prefect.utilities.processutils import (
    TextSink,
    consume_process_output,
    open_process,
)
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import BaseModel

if HAS_PYDANTIC_V2:
    from pydantic.v1 import Field, validator
else:
    from pydantic import Field, validator


class SlurmJobStatus(IntEnum):
    Pending = 0
    Running = 1
    Suspended = 2
    Complete = 3
    Cancelled = 4
    Failed = 5
    Timeout = 6
    Nodefail = 7
    Preempted = 8
    Bootfail = 9
    Deadline = 10
    Oom = 11
    End = 12

    @classmethod
    def waitable(cls) -> list["SlurmJobStatus"]:
        return [cls.Pending, cls.Preempted, cls.Running]


# class SlurmJobDefinition(BaseModel):
#     partition: str | None = Field(
#         default=None,
#         description="Partition on which the job will run",
#     )
#     time_limit: timedelta | None = Field(
#         default=None,
#         # default=timedelta(hours=1),
#         # default="01:00:00",
#         description="Maximum Walltime",
#     )
#     tasks: int | None = Field(
#         default=None,
#         description="Number of MPI tasks",
#     )
#     name: str = Field(
#         default="prefect",
#         description="Name of the SLURM job",
#     )
#     nodes: str = Field(
#         default=1,
#         description="Number of nodes for the SLURM job",
#     )
#     current_working_directory: Path = Field(
#         description="Working directory",
#     )
#     # environment: dict[str, str] = Field(
#     #     default={
#     #         "PATH": "/bin:/usr/bin/:/usr/local/bin/",
#     #         "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
#     #     },
#     #     description="Environment variables",
#     # )
#     # output: str = Field(default="output.log")
#     # error: str = Field(default="error.log")


class SlurmJob(BaseModel):
    id: int


class SlurmJobConfiguration(BaseJobConfiguration):

    """
    SlurmJobConfiguration defines the SLURM configuration for a particular job.

    Currently only the most important options for a job are covered. This includes:

    1) which partition to run on
    2) walltime, number of nodes and number of cpus
    3) working directory
    4) a conda environment to initiate for the run (that should probably be outsourced)
    """

    stream_output: bool = Field(default=True)
    working_dir: Path | None = Field(default=None)

    num_nodes: int = Field(default=1)
    num_processes_per_node: int = Field(default=1)
    time_limit: timedelta = Field(
        default=timedelta(hours=1),
        title="Time limit",
        # default="24:00:00", pattern="^[0-9]{1,9}:[0-5][0-9]:[0-5][0-9]"
    )
    partition: str | None = Field(
        default=None,
        title="Slurm partition",
        description="The SLURM partition (queue) jobs are submitted to",
    )

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll for job updates",
    )

    @validator("working_dir")
    def validate_command(cls, v):
        """
        Make sure that the working directory is formatted for the current platform.
        """
        if v:
            return relative_path_to_current_platform(v)
        return v

    def prepare_for_flow_run(
        self,
        flow_run: FlowRun,
        deployment: DeploymentResponse | None = None,
        flow: Flow | None = None,
    ):
        """
        Prepare the flow run by setting some important environment variables and
        adjusting the execution environment.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)


class SlurmWorkerResult(BaseWorkerResult):
    """SLURM worker result class"""


class SlurmWorker(BaseWorker):
    """SLURM worker"""

    type = "slurm-worker"
    job_configuration = SlurmJobConfiguration
    _description = "SLURM worker."

    async def run(
        self,
        flow_run: FlowRun,
        configuration: SlurmJobConfiguration,
        task_status: anyio.abc.TaskStatus | None = None,
    ) -> SlurmWorkerResult:
        flow_run_logger = self.get_flow_run_logger(flow_run)
        flow_run_logger.debug(f"configuration.command:\n{configuration.command}")
        script = self._submit_script(configuration)
        flow_run_logger.debug(f"script:\n{script}")
        job = await self._create_and_start_job(
            script,
            configuration,
            flow_run_logger,
        )
        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to tear down infrastructure
            # if the flow run is cancelled.
            task_status.started(job.id)
        job_status = await self._watch_job(job, configuration)
        exit_code = job_status.value if job_status else -1
        return SlurmWorkerResult(
            status_code=exit_code,
            identifier=job.id,
        )

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: BaseJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        # Tear down the execution environment
        print(infrastructure_pid)
        return
        # await self._kill_job(infrastructure_pid, configuration)

    def _submit_script(self, configuration: SlurmJobConfiguration) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]

        command = configuration.command or self._base_flow_run_command()
        script += [command]

        return "\n".join(script)

    @staticmethod
    def _base_flow_run_command() -> str:
        """
        Generate a command for a flow run job.
        """
        return "python -m prefect.engine"

    async def _create_and_start_job(
        self,
        script: str,
        configuration: SlurmJobConfiguration,
        logger: PrefectLogAdapter,
    ) -> SlurmJob:
        command = ["sbatch", "--parsable"]
        command.append(f"--nodes={configuration.num_nodes}")
        command.append(f"--ntasks={configuration.num_processes_per_node}")
        command.append(f"--time={configuration.time_limit.seconds // 60}")
        if configuration.partition is not None:
            command.append(f"--partition={configuration.partition}")
        if configuration.working_dir is not None:
            command.append(f"--chdir={configuration.working_dir.as_posix()}")
        logger.debug(f"Command:\n{' '.join(command)}")
        logger.debug(f"Script:\n{script}")
        process = await run_process_pipe_script(
            command=command,
            script=script,
            logger=logger,
            stream_output=configuration.stream_output,
            env=configuration.env,
        )
        logger.debug(process.returncode)
        logger.debug(vars(process))
        return SlurmJob(id=0)

    async def _get_job_status(self, job: SlurmJob) -> SlurmJobStatus:
        command = ["squeue", f"--job={job.id}", "--long", "--noheader"]
        return SlurmJobStatus.Failed

    async def _watch_job(
        self, job: SlurmJob, configuration: BaseJobConfiguration
    ) -> SlurmJobStatus:
        await asyncio.sleep(configuration.update_interval_sec)
        while (
            status := await self._get_job_status(job)
        ) in SlurmJobStatus.waitable():
            await asyncio.sleep(configuration.update_interval_sec)
        return status


async def run_process_pipe_script(
    command: list[str],
    script: str | None = None,
    logger: PrefectLogAdapter | None = None,
    stream_output: bool | tuple[TextSink | None, TextSink | None] = False,
    **kwargs,
):
    """
    Like `anyio.run_process` but with:

    - Use of our `open_process` utility to ensure resources are cleaned up
    - Simple `stream_output` support to connect the subprocess to the parent stdout/err
    - Support for submission with `TaskGroup.start` marking as 'started' after the
        process has been created. When used, the PID is returned to the task status.

    """
    if stream_output is True:
        stream_output = (sys.stdout, sys.stderr)

    async with open_process(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE if stream_output else subprocess.DEVNULL,
        stderr=subprocess.PIPE if stream_output else subprocess.DEVNULL,
        **kwargs,
    ) as process:
        if logger is None:
            debug = print
        else:
            debug = logger.debug
        debug(f"Command sent to {process.pid}")
        if script is not None:
            if process.stdin is not None:
                debug(f"Sending script to {process.pid} stdin")
                await process.stdin.send(script.encode())
                await process.stdin.aclose()
            else:
                raise ValueError("cannot reach stdin")

        if stream_output:
            debug(f"Streaming output of {process.pid}")
            await consume_process_output(
                process,
                stdout_sink=stream_output[0],
                stderr_sink=stream_output[1],
            )

        debug(f"Waiting {process.pid}")
        await process.wait()

    return process
