import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo

from snakemake_executor_plugin_pbs import (
    Executor,
    ExecutorSettings,
    _optional_positive_int,
    _sanitize_job_name,
    _walltime,
    common_settings,
)
from snakemake_executor_plugin_pbs.pbs import (
    JobSubmission,
    PbsClient,
    PbsCommandError,
    PbsConfig,
    PbsJobStatus,
    ResourceMode,
    run_command,
)


class FakeRunner:
    def __init__(self, results: list[subprocess.CompletedProcess[str]]):
        self.results = iter(results)
        self.calls: list[tuple[list[str], int]] = []

    def __call__(
        self, args: list[str], timeout: int
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((args, timeout))
        return next(self.results)


def completed(
    stdout: str = "", stderr: str = "", returncode: int = 0
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_submit_uses_modern_pbs_resources_and_returns_job_id(tmp_path: Path) -> None:
    runner = FakeRunner([completed("1234.server\n")])
    client = PbsClient(
        PbsConfig(
            queue="workq",
            account="project",
            export_environment=True,
            mail_user="user@example.org",
            mail_events="ae",
            extra_qsub_args=("-r", "y"),
        ),
        runner=runner,
    )
    script = tmp_path / "job.sh"

    job_id = client.submit(
        JobSubmission(
            script=script,
            name="smk-job",
            threads=8,
            memory_mb=4096,
            walltime="01:30:00",
            stdout=tmp_path / "job.out",
            stderr=tmp_path / "job.err",
        )
    )

    assert job_id == "1234.server"
    assert runner.calls == [
        (
            [
                "qsub",
                "-N",
                "smk-job",
                "-l",
                "select=1:ncpus=8:mem=4096mb",
                "-l",
                "walltime=01:30:00",
                "-q",
                "workq",
                "-A",
                "project",
                "-o",
                str(tmp_path / "job.out"),
                "-e",
                str(tmp_path / "job.err"),
                "-V",
                "-M",
                "user@example.org",
                "-m",
                "ae",
                "-r",
                "y",
                str(script),
            ],
            60,
        )
    ]


def test_submit_supports_legacy_nodes_resources(tmp_path: Path) -> None:
    runner = FakeRunner([completed("42.cluster")])
    client = PbsClient(PbsConfig(resource_mode=ResourceMode.NODES), runner=runner)

    client.submit(
        JobSubmission(
            script=tmp_path / "job.sh",
            name="job",
            threads=2,
            memory_mb=None,
            walltime=None,
            stdout=tmp_path / "job.out",
            stderr=tmp_path / "job.err",
        )
    )

    assert runner.calls[0][0][4] == "nodes=1:ppn=2"


def test_status_maps_running_and_finished_jobs() -> None:
    runner = FakeRunner(
        [
            completed("Job Id: 1.server\n    job_state = R\n"),
            completed("Job Id: 2.server\n    job_state = F\n    Exit_status = 0\n"),
            completed("Job Id: 3.server\n    job_state = F\n    Exit_status = 271\n"),
        ]
    )
    client = PbsClient(PbsConfig(), runner=runner)

    assert client.status("1.server") == PbsJobStatus.ACTIVE
    assert client.status("2.server") == PbsJobStatus.SUCCESS
    assert client.status("3.server") == PbsJobStatus.FAILED
    assert [call[0] for call in runner.calls] == [
        ["qstat", "-f", "-x", "1.server"],
        ["qstat", "-f", "-x", "2.server"],
        ["qstat", "-f", "-x", "3.server"],
    ]


def test_status_rejects_unknown_state() -> None:
    client = PbsClient(
        PbsConfig(),
        runner=FakeRunner([completed("job_state = Z\n")]),
    )

    try:
        client.status("1.server")
    except PbsCommandError as error:
        assert "unknown PBS job state" in str(error)
    else:
        raise AssertionError("unknown PBS state was accepted")


def test_submit_rejects_invalid_job_id(tmp_path: Path) -> None:
    client = PbsClient(PbsConfig(), runner=FakeRunner([completed("not a job id")]))

    try:
        client.submit(
            JobSubmission(
                script=tmp_path / "job.sh",
                name="job",
                threads=1,
                memory_mb=None,
                walltime=None,
                stdout=tmp_path / "job.out",
                stderr=tmp_path / "job.err",
            )
        )
    except PbsCommandError as error:
        assert "job ID" in str(error)
    else:
        raise AssertionError("invalid qsub output was accepted")


def test_command_failure_includes_scheduler_message() -> None:
    client = PbsClient(
        PbsConfig(),
        runner=FakeRunner([completed(stderr="unknown job", returncode=153)]),
    )

    try:
        client.status("1.server")
    except PbsCommandError as error:
        assert "unknown job" in str(error)
    else:
        raise AssertionError("qstat failure was accepted")


def test_cancel_attempts_every_job() -> None:
    runner = FakeRunner(
        [
            completed(),
            completed(stderr="permission denied", returncode=1),
        ]
    )
    client = PbsClient(PbsConfig(), runner=runner)

    failures = client.cancel(["1.server", "2.server"])

    assert failures == {"2.server": "permission denied"}
    assert [call[0] for call in runner.calls] == [
        ["qdel", "1.server"],
        ["qdel", "2.server"],
    ]


class FakePbsClient:
    def __init__(
        self,
        job_id: str = "99.server",
        statuses: list[PbsJobStatus | Exception] | None = None,
        cancellation_failures: dict[str, str] | None = None,
    ) -> None:
        self.job_id = job_id
        self.statuses = iter(statuses or [])
        self.cancellation_failures = cancellation_failures or {}
        self.submissions: list[JobSubmission] = []
        self.cancelled: list[str] = []

    def submit(self, submission: JobSubmission) -> str:
        self.submissions.append(submission)
        return self.job_id

    def status(self, job_id: str) -> PbsJobStatus:
        result = next(self.statuses)
        if isinstance(result, Exception):
            raise result
        return result

    def cancel(self, job_ids: list[str]) -> dict[str, str]:
        self.cancelled.extend(job_ids)
        return self.cancellation_failures


class AsyncLimiter:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


def make_executor(client: FakePbsClient, retries: int = 3) -> Executor:
    executor = object.__new__(Executor)
    executor.pbs = client
    executor.executor_settings = ExecutorSettings(status_attempts=retries)
    executor.status_rate_limiter = AsyncLimiter()
    executor.logger = SimpleNamespace(errors=[], error=lambda message: None)
    return executor


def test_executor_submits_a_job_with_standard_snakemake_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    client = FakePbsClient()
    executor = make_executor(client)
    jobscript = tmp_path / "job.sh"
    executor.get_jobscript = lambda job: str(jobscript)
    executor.get_jobname = lambda job: "snakejob.rule/unsafe.7"
    executor.write_jobscript = lambda job, path: Path(path).write_text("#!/bin/sh\n")
    submitted: list[SubmittedJobInfo] = []
    executor.report_job_submission = submitted.append
    job = SimpleNamespace(
        jobid=7,
        threads=4,
        resources={"mem_mb": 8192, "runtime": 90},
        logfile_suggestion=lambda prefix: str(Path(prefix) / "rule-7"),
    )

    executor.run_job(job)

    launcher = Path(f"{jobscript}.pbs")
    assert jobscript.read_text() == "#!/bin/sh\n"
    assert launcher.read_text() == (
        "#!/bin/sh\n"
        f"cd {shlex.quote(str(tmp_path))} || exit 1\n"
        f"exec {shlex.quote(str(jobscript))}\n"
    )
    assert client.submissions == [
        JobSubmission(
            script=launcher,
            name="snakejob-rule-unsafe-7",
            threads=4,
            memory_mb=8192,
            walltime="01:30:00",
            stdout=tmp_path / "pbs" / "rule-7.out",
            stderr=tmp_path / "pbs" / "rule-7.err",
        )
    ]
    assert submitted[0].job is job
    assert submitted[0].external_jobid == "99.server"


@pytest.mark.asyncio
async def test_executor_reports_terminal_states_and_yields_active_jobs() -> None:
    client = FakePbsClient(
        statuses=[
            PbsJobStatus.ACTIVE,
            PbsJobStatus.SUCCESS,
            PbsJobStatus.FAILED,
        ]
    )
    executor = make_executor(client)
    successful: list[SubmittedJobInfo] = []
    failed: list[SubmittedJobInfo] = []
    executor.report_job_success = successful.append
    executor.report_job_error = lambda job, **kwargs: failed.append(job)
    jobs = [
        SubmittedJobInfo(SimpleNamespace(), external_jobid=f"{number}.server")
        for number in range(3)
    ]

    active = [job async for job in executor.check_active_jobs(jobs)]

    assert active == [jobs[0]]
    assert successful == [jobs[1]]
    assert failed == [jobs[2]]


@pytest.mark.asyncio
async def test_executor_retries_transient_status_errors_before_failing() -> None:
    client = FakePbsClient(
        statuses=[PbsCommandError("temporary"), PbsCommandError("still unavailable")]
    )
    executor = make_executor(client, retries=2)
    errors: list[str] = []
    executor.report_job_error = lambda job, msg=None: errors.append(msg)
    job = SubmittedJobInfo(SimpleNamespace(), external_jobid="1.server")

    first = [active async for active in executor.check_active_jobs([job])]
    second = [active async for active in executor.check_active_jobs(first)]

    assert first == [job]
    assert second == []
    assert errors == ["PBS status check failed after 2 attempts: still unavailable"]


def test_executor_cancels_all_external_jobs_and_logs_failures() -> None:
    client = FakePbsClient(cancellation_failures={"2.server": "permission denied"})
    executor = make_executor(client)
    errors: list[str] = []
    executor.logger = SimpleNamespace(error=errors.append)
    jobs = [
        SubmittedJobInfo(SimpleNamespace(), external_jobid="1.server"),
        SubmittedJobInfo(SimpleNamespace(), external_jobid="2.server"),
        SubmittedJobInfo(SimpleNamespace(), external_jobid=None),
    ]

    executor.cancel_jobs(jobs)

    assert client.cancelled == ["1.server", "2.server"]
    assert errors == ["Failed to cancel PBS job 2.server: permission denied"]


def test_snakemake_cli_registers_pbs_settings() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "snakemake", "--executor", "pbs", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "--pbs-qsub" in result.stdout


def test_plugin_declares_a_shared_filesystem_executor() -> None:
    assert common_settings.non_local_exec is True
    assert common_settings.implies_no_shared_fs is False
    assert common_settings.job_deploy_sources is False
    assert common_settings.auto_deploy_default_storage_provider is False


def test_executor_initializes_a_configured_pbs_client() -> None:
    executor = object.__new__(Executor)
    executor.executor_settings = ExecutorSettings(
        qsub="/opt/pbs/bin/qsub",
        qstat="/opt/pbs/bin/qstat",
        qdel="/opt/pbs/bin/qdel",
        queue="workq",
        account="project",
        resource_mode="nodes",
        default_walltime="24:00:00",
        export_environment=True,
        mail_user="user@example.org",
        mail_events="ae",
        extra_qsub_args="-r y -W 'group_list=lab team'",
        command_timeout=30,
    )

    executor.__post_init__()

    assert executor.pbs.config == PbsConfig(
        qsub="/opt/pbs/bin/qsub",
        qstat="/opt/pbs/bin/qstat",
        qdel="/opt/pbs/bin/qdel",
        queue="workq",
        account="project",
        resource_mode=ResourceMode.NODES,
        export_environment=True,
        mail_user="user@example.org",
        mail_events="ae",
        extra_qsub_args=("-r", "y", "-W", "group_list=lab team"),
        timeout=30,
    )


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        (ExecutorSettings(command_timeout=0), "command-timeout"),
        (ExecutorSettings(status_attempts=0), "status-attempts"),
        (ExecutorSettings(resource_mode="invalid"), "invalid"),
        (ExecutorSettings(default_walltime="90 minutes"), "HH:MM:SS"),
    ],
)
def test_executor_rejects_invalid_settings(
    settings: ExecutorSettings, message: str
) -> None:
    executor = object.__new__(Executor)
    executor.executor_settings = settings

    with pytest.raises(WorkflowError, match=message):
        executor.__post_init__()


def test_executor_reports_invalid_job_resources(tmp_path: Path) -> None:
    client = FakePbsClient()
    executor = make_executor(client)
    executor.get_jobscript = lambda job: str(tmp_path / "job.sh")
    executor.get_jobname = lambda job: "job"
    executor.write_jobscript = lambda job, path: None
    errors: list[str] = []
    executor.report_job_error = lambda job, msg=None: errors.append(msg)
    job = SimpleNamespace(
        threads=1,
        resources={"mem_mb": True, "runtime": "invalid"},
        logfile_suggestion=lambda prefix: str(tmp_path / prefix / "job"),
    )

    executor.run_job(job)

    assert client.submissions == []
    assert errors == ["PBS submission failed: mem_mb must be a positive integer"]


@pytest.mark.asyncio
async def test_executor_rejects_a_job_without_an_external_id() -> None:
    executor = make_executor(FakePbsClient())
    errors: list[str] = []
    executor.report_job_error = lambda job, msg=None: errors.append(msg)

    active = [
        job
        async for job in executor.check_active_jobs(
            [SubmittedJobInfo(SimpleNamespace())]
        )
    ]

    assert active == []
    assert errors == ["PBS job has no external job ID"]


@pytest.mark.asyncio
async def test_successful_status_check_clears_retry_counter() -> None:
    executor = make_executor(FakePbsClient(statuses=[PbsJobStatus.ACTIVE]))
    job = SubmittedJobInfo(
        SimpleNamespace(),
        external_jobid="1.server",
        aux={"pbs_status_attempts": 1},
    )

    active = [item async for item in executor.check_active_jobs([job])]

    assert active == [job]
    assert job.aux == {}


def test_status_without_exit_status_remains_active() -> None:
    client = PbsClient(PbsConfig(), runner=FakeRunner([completed("job_state = F\n")]))

    assert client.status("1.server") == PbsJobStatus.ACTIVE


def test_runner_operating_system_error_is_wrapped() -> None:
    def unavailable(args: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("qstat not found")

    client = PbsClient(PbsConfig(), runner=unavailable)

    with pytest.raises(PbsCommandError, match="qstat not found"):
        client.status("1.server")


def test_executor_initializes_default_settings() -> None:
    executor = object.__new__(Executor)
    executor.executor_settings = ExecutorSettings()

    executor.__post_init__()

    assert executor.pbs.config == PbsConfig()


def test_job_name_starting_with_a_non_letter_gets_a_safe_prefix() -> None:
    assert _sanitize_job_name("123.rule") == "smk-123-rule"


def test_optional_memory_accepts_absence_and_rejects_invalid_values() -> None:
    assert _optional_positive_int(None) is None

    with pytest.raises(ValueError, match="positive integer"):
        _optional_positive_int("many")
    with pytest.raises(ValueError, match="positive integer"):
        _optional_positive_int(0)


def test_walltime_accepts_defaults_and_explicit_pbs_values() -> None:
    assert _walltime(None, None) is None
    assert _walltime(None, "02:03:04") == "02:03:04"
    assert _walltime("01:02:03", None) == "01:02:03"


@pytest.mark.parametrize("runtime", [True, "invalid", 0])
def test_walltime_rejects_invalid_runtime(runtime: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _walltime(runtime, None)


def test_run_command_invokes_subprocess_without_a_shell() -> None:
    expected = completed("123.server\n")

    with patch(
        "snakemake_executor_plugin_pbs.pbs.subprocess.run", return_value=expected
    ) as subprocess_run:
        result = run_command(["qsub", "job.sh"], timeout=12)

    assert result is expected
    subprocess_run.assert_called_once_with(
        ["qsub", "job.sh"],
        check=False,
        capture_output=True,
        text=True,
        timeout=12,
    )


def test_qsub_failure_without_diagnostics_is_reported(tmp_path: Path) -> None:
    client = PbsClient(PbsConfig(), runner=FakeRunner([completed(returncode=1)]))

    with pytest.raises(PbsCommandError, match="qsub failed"):
        client.submit(
            JobSubmission(
                script=tmp_path / "job.sh",
                name="job",
                threads=1,
                memory_mb=None,
                walltime=None,
                stdout=tmp_path / "job.out",
                stderr=tmp_path / "job.err",
            )
        )
