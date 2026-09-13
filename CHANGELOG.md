# Changelog

All notable changes to this project will be documented here.

This project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- Start PBS jobs in Snakemake's working directory instead of the user's home directory.
- Use Pixi's absolute path in PBS launchers so jobs do not depend on PBS exporting the submission `PATH`.

### Added

- Initial PBS Professional and OpenPBS executor implementation.
- Modern `select/ncpus` and legacy `nodes/ppn` resource modes.
- Safe `qsub`, `qstat`, and `qdel` command execution.
- Shared-filesystem profile example and PBS-independent test suite.
