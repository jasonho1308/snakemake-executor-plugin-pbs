# Contributing

Contributions and PBS compatibility reports are welcome.

## Development setup

1. Install [Pixi](https://pixi.sh/).
2. Fork and clone the repository.
3. Run `pixi install`.
4. Create a focused branch from `main`.

Before opening a pull request, run:

```console
pixi run -e dev format
pixi run -e dev lint
pixi run -e dev typecheck
pixi run -e dev test
pixi run -e dev check-build
```

Tests must not require access to a real PBS cluster. Add representative, sanitized scheduler output to a fake command runner instead. Never commit usernames, email addresses, credentials, private hostnames, account names, or proprietary workflow data.

Use a conventional pull-request title such as `feat: support PBS arrays` or `fix: parse completed job status`.
