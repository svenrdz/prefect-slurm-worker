import asyncio
import subprocess
import sys
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import AsyncGenerator

import anyio
import anyio.abc
from prefect.client.schemas import FlowRun
from prefect.engine import propose_state
from prefect.logging.loggers import PrefectLogAdapter
from prefect.pydantic import BaseModel, Field, field_validator
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.states import Cancelled
from prefect.utilities.filesystem import relative_path_to_current_platform
from prefect.utilities.processutils import (  # consume_process_output,
    TextSink,
    open_process,
)
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseWorker,
    BaseWorkerResult,
)


class SlurmJobStatus(str, Enum):
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    REQUEUE_FED = "REQUEUE_FED"
    REQUEUE_HOLD = "REQUEUE_HOLD"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SIGNALING = "SIGNALING"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STAGE_OUT = "STAGE_OUT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"

    @classmethod
    def waitable(cls) -> list["SlurmJobStatus"]:
        return [cls.PENDING, cls.PREEMPTED, cls.RUNNING]

    def __repr__(self) -> str:
        return self.value


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
    id: int | None
    status: SlurmJobStatus | None = None
    exit_code: int | None = None


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

    modules: list[int] = Field(
        default_factory=list,
        title="Modules",
        description="Names of modules to load for job",
    )

    conda_environment: str | None = Field(
        default=None,
        title="Conda environment",
        description="Name of conda environment",
    )

    @field_validator("working_dir")
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
    ) -> BaseWorkerResult:
        self._logger = self.get_flow_run_logger(flow_run)
        script = self._submit_script(configuration)
        job_id = await self._create_and_start_job(script, configuration, self._logger)
        self._logger.info(f"SlurmJob submitted with id: {job_id}.")
        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to tear down infrastructure
            # if the flow run is cancelled.
            task_status.started(job_id)
        job = await self._watch_job_safe(job_id, configuration, self._logger)
        if job.status == SlurmJobStatus.CANCELLED:
            state = await propose_state(
                self._client,
                Cancelled(message="This flow run has been cancelled by Slurm."),
                flow_run_id=flow_run.id,
            )
            job.exit_code = 0
        self._logger.info(f"SlurmJob ended: {job}")
        return SlurmWorkerResult(status_code=job.exit_code, identifier=job.id)

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: SlurmJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        # Tear down the execution environment
        self._logger.info(f"Sending SIGKILL to job with id: {infrastructure_pid}")
        command = [
            "scancel",
            infrastructure_pid,
            "--signal=SIGKILL",
        ]
        self._logger.info(f"{command=}")
        await run_process_pipe_script(command=command)
        # await asyncio.sleep(grace_seconds)
        # async for job in self._watch_job(infrastructure_pid, self._logger):
        #     if job.status in SlurmJobStatus.waitable():
        #         self._logger.info(
        #             f"Sending SIGKILL to job with id: {infrastructure_pid}"
        #         )
        #         command = [
        #             "scancel",
        #             f"--job={infrastructure_pid}",
        #             "--signal=SIGKILL",
        #         ]
        #         self._logger.info(f"{command=}")
        #         await run_process_pipe_script(command=command)
        #     return

    def _submit_script(self, configuration: SlurmJobConfiguration) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]

        if configuration.modules:
            script.append("module purge")
            for module_name in configuration.modules:
                script.append(f"module load {module_name}")

        if configuration.conda_environment is not None:
            script.append(f"conda activate {configuration.conda_environment}")

        script.append(configuration.command)

        return "\n".join(script)

    async def _create_and_start_job(
        self,
        script: str,
        configuration: SlurmJobConfiguration,
        logger: PrefectLogAdapter,
    ) -> int | None:
        command = [
            "sbatch",
            "--parsable",
            f"--nodes={configuration.num_nodes}",
            f"--ntasks={configuration.num_processes_per_node}",
            f"--time={configuration.time_limit.seconds // 60}",
        ]
        if configuration.partition is not None:
            command.append(f"--partition={configuration.partition}")
        if configuration.working_dir is not None:
            command.append(f"--chdir={configuration.working_dir.as_posix()}")
        logger.info(f"Command:\n{' '.join(command)}")
        logger.info(f"Script:\n{script}")
        # out_sink = anyio.create_memory_object_stream()
        # err_sink = anyio.create_memory_object_stream()
        output = await run_process_pipe_script(
            command=command,
            script=script,
            logger=logger,
            stream_output=True,
            # stream_output=(out_sink[0], err_sink[0]),
            env=configuration.env,
        )
        try:
            job_id = int(output.strip())
        except ValueError:
            job_id = None
        return job_id

    async def _watch_job(
        self,
        job_id: int | None,
        logger: PrefectLogAdapter,
    ) -> AsyncGenerator[SlurmJob, None]:
        if job_id is None:
            yield SlurmJob(id=job_id, status=SlurmJobStatus.FAILED, exit_code=-1)
        else:
            command = [
                "squeue",
                f"--job={job_id}",
                "--noheader",
                "--state=all",
                "--Format=State,exit_code",
            ]
            while True:
                output = await run_process_pipe_script(
                    command=command,
                    script=None,
                    stream_output=True,
                    logger=logger,
                )
                if output:
                    status, exit_code = output.strip().split()
                    yield SlurmJob(
                        id=job_id,
                        status=SlurmJobStatus(status),
                        exit_code=int(exit_code),
                    )
                else:
                    return

    async def _watch_job_safe(
        self,
        job_id: int | None,
        configuration: BaseJobConfiguration,
        logger: PrefectLogAdapter,
    ) -> SlurmJob:
        # await asyncio.sleep(configuration.update_interval_sec)
        seen_statuses = set()
        job = None
        async for job in self._watch_job(job_id, logger):
            if job.status not in seen_statuses:
                seen_statuses.add(job.status)
            if job.status not in SlurmJobStatus.waitable():
                return job
            # await asyncio.sleep(configuration.update_interval_sec)
            await asyncio.sleep(5)
        if job is None:
            return SlurmJob(id=job_id, status=SlurmJobStatus.FAILED, exit_code=-1)
        else:
            return job
        # TODO: emit change event


async def run_process_pipe_script(
    command: list[str],
    script: str | None = None,
    logger: PrefectLogAdapter | None = None,
    stream_output: bool | tuple[TextSink | None, TextSink | None] = False,
    **kwargs,
) -> str:
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

        # if stream_output:
        #     debug(f"Streaming output of {process.pid}")
        #     await consume_process_output(
        #         process,
        #         stdout_sink=stream_output[0],
        #         stderr_sink=stream_output[1],
        #     )

        debug(f"Waiting {process.pid}")
        await process.wait()
        if process.stdout is not None:
            try:
                return (await process.stdout.receive()).decode()
            except anyio.EndOfStream:
                return ""
        else:
            return ""
