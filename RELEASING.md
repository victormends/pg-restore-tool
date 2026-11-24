# Releasing `pg_restore_tool`

## Public Release Checklist

Before tagging a public release, verify:

- `py -m compileall .` completes successfully.
- `py main.py --help` matches the usage examples in `README.md`.
- The package version in `pyproject.toml` matches the intended release tag.
- `LICENSE` and `SECURITY.md` are present.
- A sanitization scan confirms only generic hostnames, database names, and backup paths appear in configs, tests, and docs.
- Release notes describe this as an operator-guided Windows restore utility, not a guaranteed production disaster-recovery system.
