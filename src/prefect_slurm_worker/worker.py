import asyncio
import contextlib
import os
import subprocess
import sys
import tempfile
from enum import Enum
from pathlib import Path
from typing import AsyncGenerator

import anyio
import anyio.abc
import pendulum
from prefect.client.schemas import FlowRun
from prefect.engine import propose_state
from prefect.logging.loggers import PrefectLogAdapter
from prefect.pydantic import BaseModel, Field, field_validator
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.states import Cancelled
from prefect.utilities.filesystem import relative_path_to_current_platform
from prefect.utilities.processutils import TextSink, open_process
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
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
    working_dir: Path | None = Field(default=None, description="Slurm job chdir")
    log_path: Path | None = Field(default=None, description="Slurm job output.")

    num_nodes: int = Field(default=1)
    num_processes_per_node: int = Field(default=1)
    time_limit: pendulum.Duration = Field(
        default=pendulum.Duration(hours=1),
        description="Slurm job time limit (in seconds)",
        # default="24:00:00", pattern="^[0-9]{1,9}:[0-5][0-9]:[0-5][0-9]"
    )
    partition: str | None = Field(
        default=None,
        title="Slurm partition",
        description="The SLURM partition (queue) jobs are submitted to.",
    )
    memory: str | None = Field(
        default=None,
        pattern="^[0-9]{1,9}[M|G]$",
        title="Slurm memory limit",
        description="The SLURM memory for the job.",
    )

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll for job updates.",
    )

    modules: list[str] = Field(
        default_factory=list,
        title="Modules",
        description="Names of modules to load for slurm job.",
    )

    conda_environment: str | None = Field(
        default=None,
        title="Conda environment",
        description="Name of conda environment loaded inside slurm job.",
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


class SlurmJobVariables(BaseVariables):
    """
    SlurmJobVariables define the set of variables that can be defined at
    submission time of a new job and will be used to template a SlurmJobConfiguration.
    """

    stream_output: bool = Field(default=True)
    working_dir: Path | None = Field(default=None, description="Slurm job chdir")
    log_path: Path | None = Field(default=None, description="Slurm job output.")

    num_nodes: int = Field(default=1)
    num_processes_per_node: int = Field(default=1)
    time_limit: pendulum.Duration = Field(
        default=pendulum.Duration(hours=1),
        description="Slurm job time limit (in seconds)",
        # default="24:00:00", pattern="^[0-9]{1,9}:[0-5][0-9]:[0-5][0-9]"
    )
    partition: str | None = Field(
        default=None,
        title="Slurm partition",
        description="The SLURM partition (queue) jobs are submitted to.",
    )
    memory: str | None = Field(
        default=None,
        pattern="^[0-9]{1,9}[M|G]$",
        title="Slurm memory limit",
        description="The SLURM memory for the job.",
    )

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll for job updates.",
    )

    modules: list[str] = Field(
        default_factory=list,
        title="Modules",
        description="Names of modules to load for slurm job.",
    )

    conda_environment: str | None = Field(
        default=None,
        title="Conda environment",
        description="Name of conda environment loaded inside slurm job.",
    )


class SlurmWorkerResult(BaseWorkerResult):
    """SLURM worker result class"""


class SlurmWorker(BaseWorker):
    """SLURM worker"""

    type = "slurm-worker"
    job_configuration = SlurmJobConfiguration
    job_configuration_variables = SlurmJobVariables
    _description = "SLURM worker."

    async def run(
        self,
        flow_run: FlowRun,
        configuration: SlurmJobConfiguration,
        task_status: anyio.abc.TaskStatus | None = None,
    ) -> BaseWorkerResult:
        self._logger = self.get_flow_run_logger(flow_run)
        job_id = await self._create_and_start_job(configuration)
        self.log_info(f"SlurmJob submitted with id: {job_id}.")
        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to tear down infrastructure
            # if the flow run is cancelled.
            task_status.started(job_id)
        job = await self._watch_job_safe(job_id, configuration)
        if job.status == SlurmJobStatus.CANCELLED and self._client is not None:
            await propose_state(
                self._client,
                Cancelled(message="This flow run has been cancelled by Slurm."),
                flow_run_id=flow_run.id,
            )
            job.exit_code = 0
        self.log_info(f"SlurmJob ended: {job}")
        return SlurmWorkerResult(status_code=job.exit_code, identifier=job.id)

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: SlurmJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        # Tear down the execution environment
        command = [
            "scancel",
            infrastructure_pid,
            "--signal=SIGKILL",
        ]
        self.log_info(f"Sending SIGKILL to job with id: {infrastructure_pid}")
        self.log_info(f"{command=}")
        await run_process_pipe_script(command=command)
        # await asyncio.sleep(grace_seconds)
        # async for job in self._watch_job(infrastructure_pid):
        #     if job.status in SlurmJobStatus.waitable():
        #         self.log_info(
        #             f"Sending SIGKILL to job with id: {infrastructure_pid}"
        #         )
        #         command = [
        #             "scancel",
        #             f"--job={infrastructure_pid}",
        #             "--signal=SIGKILL",
        #         ]
        #         self.log_info(f"{command=}")
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
        self, configuration: SlurmJobConfiguration
    ) -> int | None:
        script = self._submit_script(configuration)
        command = [
            "sbatch",
            "--parsable",
            f"--nodes={configuration.num_nodes}",
            f"--ntasks={configuration.num_processes_per_node}",
            f"--time={configuration.time_limit.seconds // 60}",
        ]
        if configuration.memory is not None:
            command.append(f"--mem={configuration.memory}")
        if configuration.partition is not None:
            command.append(f"--partition={configuration.partition}")
        if configuration.log_path is not None:
            command.append(f"--output={configuration.log_path}")
        working_dir_ctx = (
            tempfile.TemporaryDirectory(suffix="prefect")
            if not configuration.working_dir
            else contextlib.nullcontext(configuration.working_dir)
        )
        with working_dir_ctx as working_dir:
            command.append(f"--chdir={working_dir}")
            self.log_info(f"Command:\n{' '.join(command)}")
            self.log_info(f"Script:\n{script}")
            output = await run_process_pipe_script(
                command=command,
                script=script,
                logger=self.logger,
                stream_output=True,
                env=os.environ | configuration.env,
            )
        try:
            job_id = int(output.strip())
        except ValueError:
            job_id = None
        return job_id

    async def _watch_job(self, job_id: int | None) -> AsyncGenerator[SlurmJob, None]:
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
                    logger=self.logger,
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
    ) -> SlurmJob:
        seen_statuses = set()
        job = None
        async for job in self._watch_job(job_id):
            if job.status not in seen_statuses:
                seen_statuses.add(job.status)
            if job.status not in SlurmJobStatus.waitable():
                return job
            await asyncio.sleep(configuration.update_interval_sec)
        if job is None:
            return SlurmJob(id=job_id, status=SlurmJobStatus.FAILED, exit_code=-1)
        else:
            return job

    @property
    def logger(self) -> PrefectLogAdapter | None:
        if hasattr(self, "_logger"):
            return self._logger
        else:
            return None

    def log_info(self, msg: str):
        logger = self.logger
        if logger is not None:
            logger.info(msg)
        else:
            print(msg)


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
