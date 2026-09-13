# snakemake-executor-plugin-pbs

[![CI](https://github.com/jasonho1308/snakemake-executor-plugin-pbs/actions/workflows/ci.yml/badge.svg)](https://github.com/jasonho1308/snakemake-executor-plugin-pbs/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/snakemake-executor-plugin-pbs.svg)](https://pypi.org/project/snakemake-executor-plugin-pbs/)

A [Snakemake](https://snakemake.readthedocs.io/) executor plugin for PBS Professional and OpenPBS clusters. It submits jobs with `qsub`, monitors retained job records with `qstat`, and cancels jobs with `qdel`.


The plugin was designed for PBS Professional 19 and uses the modern PBS `select` resource syntax by default. A legacy `nodes/ppn` mode is available for older PBS and Torque installations.

## Installation

Before the package is published, clone the repository and install it privately with Pixi:

```bash
pixi add --pypi "snakemake-executor-plugin-pbs @ git+https://github.com/jasonho1308/snakemake-executor-plugin-pbs"
```

or

```console
git clone https://github.com/jasonho1308/snakemake-executor-plugin-pbs.git
cd snakemake-executor-plugin-pbs
pixi install
```

After a public release, it can be installed from PyPI:

```console
pip install snakemake-executor-plugin-pbs
```

For local development:

```console
pixi install
pixi run -e dev test
```

## Quick start

```console
snakemake --executor pbs --jobs 100 \
  --default-resources mem_mb=1024 runtime=60
```

The standard Snakemake resources are translated as follows:

| Snakemake | PBS `select` mode | Meaning |
| --- | --- | --- |
| `threads` | `ncpus` | CPUs requested for the job |
| `mem_mb` | `mem=<value>mb` | Memory requested for the job |
| `runtime` | `walltime=HH:MM:SS` | Runtime in minutes |

For example, `threads: 8`, `mem_mb=16384`, and `runtime=90` produce:

```text
-l select=1:ncpus=8:mem=16384mb -l walltime=01:30:00
```

## Profile configuration

Create `profiles/pbs/config.yaml`:

```yaml
executor: pbs
jobs: 100
latency-wait: 60

# Site-specific values are optional.
pbs-queue: workq
pbs-account: my-project
pbs-default-walltime: "24:00:00"

# Applied only when a rule does not define these resources.
default-resources:
  - mem_mb=1024
  - runtime=60
```

Run it with:

```console
snakemake --profile profiles/pbs
```

## Settings

All settings are available as `--pbs-<name>` CLI options and equivalent profile keys.

| Setting | Default | Description |
| --- | --- | --- |
| `qsub`, `qstat`, `qdel` | Command from `PATH` | Override PBS executable paths |
| `queue` | unset | Default PBS queue |
| `account` | unset | PBS account or project |
| `resource-mode` | `select` | `select` or legacy `nodes` syntax |
| `default-walltime` | unset | `HH:MM:SS` fallback when `runtime` is absent |
| `export-environment` | `false` | Add `-V`; enable only if compute jobs need the full submission environment |
| `pixi-environment` | unset | Run each jobscript in the named Pixi environment |
| `mail-user` | unset | PBS notification address |
| `mail-events` | unset | PBS mail event letters, such as `abe` |
| `extra-qsub-args` | unset | Shell-like string of additional `qsub` arguments; no shell is invoked |
| `command-timeout` | `60` | Per-command timeout in seconds |
| `status-attempts` | `3` | Consecutive `qstat` failures before a job is failed |

Example with additional submission options:

```console
snakemake --executor pbs --jobs 50 \
  --pbs-extra-qsub-args="-r y"
```

To run submitted jobs in a Pixi environment:

```console
snakemake --executor pbs --jobs 50 \
  --pbs-pixi-environment dev
```

The `pixi` executable found on the submission host must be accessible at the
same absolute path on compute nodes. The plugin writes that path into the launcher
and runs the generated jobscript with `pixi run --environment dev --frozen
--executable`, so the workspace lock file must also be up to date and accessible
on the shared filesystem.

### Legacy resource syntax

Use this only when the scheduler expects `nodes=1:ppn=...`:

```yaml
pbs-resource-mode: nodes
```

A job with four threads and 8 GiB then uses:

```text
-l nodes=1:ppn=4,mem=8192mb
```

## Job states and logs

Queued, held, waiting, running, suspended, and exiting jobs remain active in Snakemake. A final PBS state succeeds only when `Exit_status` is `0`; other exit statuses fail the Snakemake job.

Scheduler stdout and stderr are written to Snakemake's suggested PBS log location with `.out` and `.err` suffixes. Rule-level Snakemake logs remain controlled by the workflow. Jobs start in the workflow's working directory, so relative input and output paths resolve where Snakemake expects them.

Temporary scheduler errors are retried. Persistent errors include the PBS diagnostic in the Snakemake job error. Commands are executed directly without a shell.

## Cluster smoke test

After installing on a PBS login node, use a small workflow:

```python
rule all:
    input: "hello.txt"

rule hello:
    output: "hello.txt"
    threads: 1
    resources:
        mem_mb=128,
        runtime=5
    shell:
        "echo hello > {output}"
```

Run:

```console
snakemake --executor pbs --jobs 1 --printshellcmds
```

Confirm that the job appears in `qstat`, `hello.txt` is created, and the `.snakemake` PBS logs contain no scheduler errors.

## Limitations

- The plugin assumes a shared filesystem and does not transfer workflow files.
- Completed-job detection requires PBS history through `qstat -x`.
- Site-specific resources beyond CPU, memory, and walltime should currently be supplied with `extra-qsub-args` or configured by the PBS queue.

## Development

```console
pixi run -e dev format
pixi run -e dev lint
pixi run -e dev typecheck
pixi run -e dev test
pixi run -e dev check-build
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.



## Security

Report vulnerabilities according to [SECURITY.md](SECURITY.md). Do not include credentials or sensitive cluster output in public reports.

## License

MIT © Ho Cheuk Hai Jason
