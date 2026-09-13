"""Snakemake executor plugin for PBS Professional and OpenPBS."""

# ruff: noqa: UP045

import asyncio
import re
import shlex
import shutil
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, cast

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from snakemake_executor_plugin_pbs.pbs import (
    JobSubmission,
    PbsClient,
    PbsCommandError,
    PbsConfig,
    PbsJobStatus,
    ResourceMode,
)


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    """Settings exposed as ``--pbs-*`` Snakemake command-line options."""

    qsub: Optional[str] = field(
        default="qsub", metadata={"help": "Path to the qsub executable."}
    )
    qstat: Optional[str] = field(
        default="qstat", metadata={"help": "Path to the qstat executable."}
    )
    qdel: Optional[str] = field(
        default="qdel", metadata={"help": "Path to the qdel executable."}
    )
    queue: Optional[str] = field(default=None, metadata={"help": "Default PBS queue."})
    account: Optional[str] = field(
        default=None, metadata={"help": "Default PBS account or project."}
    )
    resource_mode: Optional[str] = field(
        default="select",
        metadata={
            "help": "PBS resource syntax. Use select for PBS Pro/OpenPBS or nodes "
            "for legacy PBS/Torque.",
            "choices": ["select", "nodes"],
        },
    )
    default_walltime: Optional[str] = field(
        default=None,
        metadata={
            "help": "Walltime used when a job has no runtime resource (HH:MM:SS)."
        },
    )
    export_environment: Optional[bool] = field(
        default=False,
        metadata={"help": "Pass -V to qsub to export the submission environment."},
    )
    pixi_environment: Optional[str] = field(
        default=None,
        metadata={
            "help": "Run submitted jobs in this Pixi environment using pixi run."
        },
    )
    mail_user: Optional[str] = field(
        default=None, metadata={"help": "Email address for PBS notifications."}
    )
    mail_events: Optional[str] = field(
        default=None,
        metadata={"help": "PBS mail events, for example abe. Disabled by default."},
    )
    extra_qsub_args: Optional[str] = field(
        default=None,
        metadata={
            "help": "Shell-like string of additional qsub arguments. The command is "
            "still executed without a shell."
        },
    )
    command_timeout: Optional[int] = field(
        default=60,
        metadata={"help": "Timeout in seconds for each PBS command."},
    )
    status_attempts: Optional[int] = field(
        default=3,
        metadata={"help": "Consecutive failed qstat attempts before a job is failed."},
    )


common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=False,
    init_seconds_before_status_checks=0,
    pass_group_args=True,
)


class Executor(RemoteExecutor):
    """Submit, monitor, and cancel Snakemake jobs through PBS."""

    def __post_init__(self) -> None:
        settings = cast(ExecutorSettings, self.executor_settings)
        timeout = settings.command_timeout or 0
        status_attempts = settings.status_attempts or 0
        if timeout <= 0:
            raise WorkflowError("--pbs-command-timeout must be greater than zero")
        if status_attempts <= 0:
            raise WorkflowError("--pbs-status-attempts must be greater than zero")
        try:
            mode = ResourceMode(settings.resource_mode or "select")
            if settings.default_walltime is not None:
                _validate_walltime(settings.default_walltime)
            extra_qsub_args = shlex.split(settings.extra_qsub_args or "")
        except ValueError as error:
            raise WorkflowError(str(error)) from error

        self.pbs = PbsClient(
            PbsConfig(
                qsub=settings.qsub or "qsub",
                qstat=settings.qstat or "qstat",
                qdel=settings.qdel or "qdel",
                queue=settings.queue,
                account=settings.account,
                resource_mode=mode,
                export_environment=bool(settings.export_environment),
                mail_user=settings.mail_user,
                mail_events=settings.mail_events,
                extra_qsub_args=tuple(extra_qsub_args),
                timeout=timeout,
            )
        )

    def run_job(self, job: JobExecutorInterface) -> None:
        jobscript = Path(self.get_jobscript(job))
        logfile = Path(job.logfile_suggestion("pbs")).resolve()
        stdout = Path(f"{logfile}.out")
        stderr = Path(f"{logfile}.err")
        stdout.parent.mkdir(parents=True, exist_ok=True)
        self.write_jobscript(job, str(jobscript))
        settings = cast(ExecutorSettings, self.executor_settings)
        launcher = _write_job_launcher(
            jobscript, Path.cwd(), pixi_environment=settings.pixi_environment
        )

        try:
            external_jobid = self.pbs.submit(
                JobSubmission(
                    script=launcher,
                    name=_sanitize_job_name(self.get_jobname(job)),
                    threads=job.threads,
                    memory_mb=_optional_positive_int(job.resources.get("mem_mb")),
                    walltime=_walltime(
                        job.resources.get("runtime"), settings.default_walltime
                    ),
                    stdout=stdout,
                    stderr=stderr,
                )
            )
        except (PbsCommandError, TypeError, ValueError) as error:
            self.report_job_error(
                SubmittedJobInfo(job), msg=f"PBS submission failed: {error}"
            )
            return

        self.report_job_submission(
            SubmittedJobInfo(job=job, external_jobid=external_jobid)
        )

    async def check_active_jobs(
        self, active_jobs: list[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        settings = cast(ExecutorSettings, self.executor_settings)
        max_attempts = settings.status_attempts or 3

        for active_job in active_jobs:
            job_id = active_job.external_jobid
            if job_id is None:
                self.report_job_error(active_job, msg="PBS job has no external job ID")
                continue

            try:
                async with self.status_rate_limiter:
                    status = await asyncio.to_thread(self.pbs.status, job_id)
            except PbsCommandError as error:
                aux = active_job.aux if active_job.aux is not None else {}
                attempts = int(aux.get("pbs_status_attempts", 0)) + 1
                aux["pbs_status_attempts"] = attempts
                active_job.aux = aux
                if attempts < max_attempts:
                    yield active_job
                else:
                    self.report_job_error(
                        active_job,
                        msg=(
                            f"PBS status check failed after {attempts} attempts: "
                            f"{error}"
                        ),
                    )
                continue

            if active_job.aux is not None:
                active_job.aux.pop("pbs_status_attempts", None)
            if status == PbsJobStatus.ACTIVE:
                yield active_job
            elif status == PbsJobStatus.SUCCESS:
                self.report_job_success(active_job)
            else:
                self.report_job_error(active_job)

    def cancel_jobs(self, active_jobs: list[SubmittedJobInfo]) -> None:
        job_ids = [
            job.external_jobid for job in active_jobs if job.external_jobid is not None
        ]
        for job_id, message in self.pbs.cancel(job_ids).items():
            self.logger.error(f"Failed to cancel PBS job {job_id}: {message}")


def _write_job_launcher(
    jobscript: Path, working_directory: Path, pixi_environment: str | None = None
) -> Path:
    launcher = Path(f"{jobscript}.pbs")
    command = shlex.quote(str(jobscript))
    if pixi_environment is not None:
        pixi_executable = shutil.which("pixi")
        if pixi_executable is None:
            raise WorkflowError(
                "--pbs-pixi-environment requires pixi to be available on PATH"
            )
        command = (
            f"{shlex.quote(pixi_executable)} run --environment "
            f"{shlex.quote(pixi_environment)} --frozen --executable {command}"
        )
    launcher.write_text(
        "#!/bin/sh\n"
        f"cd {shlex.quote(str(working_directory))} || exit 1\n"
        f"exec {command}\n"
    )
    launcher.chmod(0o700)
    return launcher


def _sanitize_job_name(name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", name).strip("-")
    if not sanitized or not sanitized[0].isalpha():
        sanitized = f"smk-{sanitized}"
    return sanitized[:200]


def _optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("mem_mb must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("mem_mb must be a positive integer") from error
    if parsed <= 0:
        raise ValueError("mem_mb must be a positive integer")
    return parsed


def _walltime(runtime: object, default: str | None) -> str | None:
    if runtime is None:
        if default is not None:
            _validate_walltime(default)
        return default
    if isinstance(runtime, bool):
        raise TypeError("runtime must be a positive number of minutes")
    if isinstance(runtime, str) and ":" in runtime:
        _validate_walltime(runtime)
        return runtime
    try:
        minutes = int(runtime)
    except (TypeError, ValueError) as error:
        raise ValueError("runtime must be minutes or HH:MM:SS") from error
    if minutes <= 0:
        raise ValueError("runtime must be a positive number of minutes")
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours:02d}:{remaining_minutes:02d}:00"


def _validate_walltime(value: str) -> None:
    match = re.fullmatch(r"(\d+):([0-5]\d):([0-5]\d)", value)
    if match is None:
        raise ValueError("walltime must use HH:MM:SS format")
