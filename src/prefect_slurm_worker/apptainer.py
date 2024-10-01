from enum import Enum
from os import PathLike
from typing import Optional
from warnings import warn

from prefect.utilities.dockerutils import get_prefect_image_name
from prefect_docker import DockerRegistryCredentials
from pydantic import AnyUrl, Field

from prefect_slurm_worker.worker import (
    SlurmJob,
    SlurmJobConfiguration,
    SlurmJobStatus,
    SlurmJobVariables,
    SlurmWorker,
    SlurmWorkerResult,
)


class ImageType(str, Enum):
    Apptainer = "apptainer"
    Docker = "docker"


class ApptainerSlurmJobConfiguration(SlurmJobConfiguration):
    image: str = Field(
        default_factory=get_prefect_image_name,
        description="The apptainer or docker image reference of a container image to use for created jobs. "
        "If not set, the latest Prefect docker image will be used.",
        examples=["docker.io/prefecthq/prefect:3-latest"],
    )
    image_type: ImageType = Field(
        default=ImageType.Docker,
        description="Type of image, determines the URI scheme for image pulling",
    )
    binds: list[str] = Field(
        default_factory=list,
        description="List of paths to bind to the container instance",
    )
    registry_credentials: Optional[DockerRegistryCredentials] = Field(
        default=None,
        title="Docker registry credentials",
        description="Docker registry credentials, required for private registries",
    )


# class ApptainerSlurmJobVariables(SlurmJobVariables):
#     image: str = Field(
#         default_factory=get_prefect_image_name,
#         description="Apptainer image url or path",
#     )
#     image_type: ImageType = Field(
#         default=ImageType.Docker,
#         description="Type of image, determines the URI scheme for image pulling",
#     )
#     binds: list[str] = Field(
#         default_factory=list,
#         description="List of paths to bind to the container instance",
#     )
#     registry_credentials: Optional[DockerRegistryCredentials] = Field(
#         default=None,
#         title="Docker registry credentials",
#         description="Docker registry credentials, required for private registries",
#     )


class ApptainerSlurmWorker(SlurmWorker):
    type = "apptainer-slurm-worker"
    job_configuration = ApptainerSlurmJobConfiguration
    job_configuration_variables = None
    # job_configuration_variables = ApptainerSlurmJobVariables
    _description = "SLURM worker for Apptainer jobs."

    def _submit_script(self, configuration: ApptainerSlurmJobConfiguration) -> str:
        """
        Generate the submit script for the apptainer job
        """
        script = ["#!/bin/bash"]

        if configuration.working_dir:
            script.append(f"mkdir -p {configuration.working_dir}")
            script.append(f"cd {configuration.working_dir}")

        if configuration.modules:
            self.logger.warn("`modules` variable is not used for apptainer jobs")

        if configuration.conda_environment is not None:
            self.logger.warn(
                "`conda_environment` variable is not used for apptainer jobs"
            )

        if configuration.binds:
            self.logger.info("Adding binds to environment")
            configuration.env["APPTAINER_BIND"] = ",".join(configuration.binds)

        if configuration.image_type == ImageType.Docker:
            if configuration.registry_credentials:
                self.logger.info("Adding registry login details to environment")
                configuration.env["APPTAINER_DOCKER_USERNAME"] = (
                    configuration.registry_credentials.username
                )
                configuration.env["APPTAINER_DOCKER_PASSWORD"] = (
                    configuration.registry_credentials.username
                )
            image = f"docker://{configuration.image}"
        elif ImageType.Apptainer:
            image = configuration.image
        else:
            raise TypeError(configuration.image_type)

        apptainer_command = ["apptainer", "run", image]
        if configuration.command:
            apptainer_command.append(configuration.command)

        script.append(" ".join(apptainer_command))

        return "\n".join(script)
