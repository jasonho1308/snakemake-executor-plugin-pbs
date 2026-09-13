"""PBS command construction and process handling."""

import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class ResourceMode(str, Enum):
    """Resource syntax understood by the target PBS installation."""

    SELECT = "select"
    NODES = "nodes"


class PbsJobStatus(str, Enum):
    """Scheduler states relevant to Snakemake."""

    ACTIVE = "active"
    SUCCESS = "success"
    FAILED = "failed"


class PbsCommandError(RuntimeError):
    """A PBS command failed or returned an invalid response."""


@dataclass(frozen=True)
class PbsConfig:
    qsub: str = "qsub"
    qstat: str = "qstat"
    qdel: str = "qdel"
    queue: str | None = None
    account: str | None = None
    resource_mode: ResourceMode = ResourceMode.SELECT
    export_environment: bool = False
    mail_user: str | None = None
    mail_events: str | None = None
    extra_qsub_args: tuple[str, ...] = ()
    timeout: int = 60


@dataclass(frozen=True)
class JobSubmission:
    script: Path
    name: str
    threads: int
    memory_mb: int | None
    walltime: str | None
    stdout: Path
    stderr: Path


CommandRunner = Callable[[list[str], int], subprocess.CompletedProcess[str]]


def run_command(args: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
    """Run a PBS command without invoking a shell."""

    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class PbsClient:
    """Small interface around the PBS command-line tools."""

    _job_id_pattern = re.compile(r"^[0-9]+(?:\[[0-9-]*\])?(?:\.[A-Za-z0-9_.-]+)?$")
    _active_states = frozenset({"B", "E", "H", "M", "Q", "R", "S", "T", "U", "W"})
    _terminal_states = frozenset({"F", "X"})

    def __init__(self, config: PbsConfig, runner: CommandRunner = run_command):
        self.config = config
        self._runner = runner

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            result = self._runner(args, self.config.timeout)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise PbsCommandError(f"{Path(args[0]).name} failed: {error}") from error
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise PbsCommandError(
                f"{Path(args[0]).name} failed" + (f": {detail}" if detail else "")
            )
        return result

    def submit(self, job: JobSubmission) -> str:
        resources = (
            f"select=1:ncpus={job.threads}"
            if self.config.resource_mode == ResourceMode.SELECT
            else f"nodes=1:ppn={job.threads}"
        )
        if job.memory_mb is not None:
            separator = ":" if self.config.resource_mode == ResourceMode.SELECT else ","
            resources += f"{separator}mem={job.memory_mb}mb"

        args = [self.config.qsub, "-N", job.name, "-l", resources]
        if job.walltime is not None:
            args.extend(("-l", f"walltime={job.walltime}"))
        if self.config.queue is not None:
            args.extend(("-q", self.config.queue))
        if self.config.account is not None:
            args.extend(("-A", self.config.account))
        args.extend(("-o", str(job.stdout), "-e", str(job.stderr)))
        if self.config.export_environment:
            args.append("-V")
        if self.config.mail_user is not None:
            args.extend(("-M", self.config.mail_user))
        if self.config.mail_events is not None:
            args.extend(("-m", self.config.mail_events))
        args.extend(self.config.extra_qsub_args)
        args.append(str(job.script))

        result = self._run(args)
        job_id = result.stdout.strip()
        if not self._job_id_pattern.fullmatch(job_id):
            raise PbsCommandError("qsub returned an invalid PBS job ID")
        return job_id

    def status(self, job_id: str) -> PbsJobStatus:
        result = self._run([self.config.qstat, "-f", "-x", job_id])
        attributes = _parse_qstat_attributes(result.stdout)
        state = attributes.get("job_state")
        if state in self._active_states:
            return PbsJobStatus.ACTIVE
        if state in self._terminal_states:
            exit_status = attributes.get("Exit_status")
            if exit_status is None:
                return PbsJobStatus.ACTIVE
            return PbsJobStatus.SUCCESS if exit_status == "0" else PbsJobStatus.FAILED
        raise PbsCommandError(f"unknown PBS job state: {state or 'missing'}")

    def cancel(self, job_ids: Sequence[str]) -> dict[str, str]:
        failures: dict[str, str] = {}
        for job_id in job_ids:
            try:
                self._run([self.config.qdel, job_id])
            except PbsCommandError as error:
                failures[job_id] = str(error).partition(": ")[2] or str(error)
        return failures


def _parse_qstat_attributes(output: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for line in output.splitlines():
        key, separator, value = line.partition(" = ")
        if separator:
            attributes[key.strip()] = value.strip()
    return attributes
