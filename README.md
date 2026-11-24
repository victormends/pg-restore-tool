# PG Restore Tool

![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Platform](https://img.shields.io/badge/Platform-Windows-blue)
![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB)

PostgreSQL restore CLI and TUI for Windows hosts supporting `pg_restore -j` parallel restores for **directory** (`-Fd`) and **custom** (`-Fc`) archives, single-stream **tar** restores, and `psql` (`ON_ERROR_STOP=1`) for `.sql` scripts, with session-scoped restore profiles (`safe`, `fast`, `unsafe`), pre-restore archive inspection (`pg_restore --list`), and subprocess-isolated credentials.

---

## Origin & Field Results

This tool originated in a multi-database Windows PostgreSQL support fleet where rebuilding local and branch databases quickly and repeatably was a recurring operational need.

In the original environment, migrating from single-threaded restores to parallel directory-format restores (`pg_restore -j 4`) paired with session-scoped `synchronous_commit=off` and elevated `maintenance_work_mem` reduced typical restore times from `~17 min` to `~7 min`. Alongside those restore improvements, separate `pg_class`/`pg_toast` catalog audits traced `~1 GB/month` of anomalous table growth to serialized audit logs.

Note that the catalog bloat diagnostics were separate SQL operational queries not bundled in this CLI, and the field timings reflect the original support fleet rather than reproducible benchmarks from this standalone repository.

---

## Key Capabilities

- **Format-aware restore dispatch:**
  - **Custom (`.dump` / `-Fc`) and Directory (`-Fd`)**: parallel restore via `pg_restore -j <jobs>` with `--no-owner --no-acl`
  - **Tar (`.tar`)**: single-stream restore via `pg_restore` (without `-j`, as `pg_restore` does not support parallel jobs on tar archives)
  - **Plain SQL (`.sql`)**: executed via `psql -v ON_ERROR_STOP=1` so meaningful SQL errors abort immediately
- **Pre-restore validation:** inspects non-SQL archives with `pg_restore --list` before destructive database operations, and checks available disk space relative to backup size.
- **Subprocess-isolated credentials:** passes `PGPASSWORD` and session `PGOPTIONS` exclusively through the child process environment dictionary rather than mutating parent process globals or command-line arguments.
- **Explicit destructive failures:** `DROP DATABASE` and `CREATE DATABASE` fail loudly rather than masking errors.
- **Post-restore verification:** runs `ANALYZE` and lightweight table/view/function count checks after completion.

---

## Restore Profiles

The tool separates session-level tuning from cluster-wide durability changes:

| Profile | Mechanism | Scope | Intended Use |
|---|---|---|---|
| `safe` | No durability or memory overrides | None | Conservative restores where default durability must remain untouched |
| `fast` (default) | `PGOPTIONS` (`synchronous_commit=off`, bounded `maintenance_work_mem`) | Restore subprocess session only | Rebuilds needing higher throughput without altering other databases on the cluster |
| `unsafe` | Temporary `ALTER SYSTEM` (`fsync=off`, `full_page_writes=off`, `synchronous_commit=off`) restored in `finally` | Cluster-wide | Dedicated, isolated restore instances only, after explicit confirmation |

---

## Quick Start

Install dependencies in a Python 3.11+ environment:

```powershell
py -m pip install -e .
```

Review CLI options:

```powershell
py main.py --help
```

Run a dry run before executing any destructive restore:

```powershell
py main.py --file backup.dump --host 127.0.0.1 --port 5432 --user postgres --db target_db --dry-run
```

Execute a restore using the default `fast` profile:

```powershell
py main.py --file backup.dump --host 127.0.0.1 --port 5432 --user postgres --db target_db
```

Use `--turbo-mode safe` when durability tradeoffs are unacceptable. Reserve `--turbo-mode unsafe` for isolated restore instances after reviewing [`SECURITY.md`](SECURITY.md).

---

## Testing

Run the offline unit test suite (`pytest` covers restore profiles, format-specific `-j` flag behavior, subprocess credential isolation, backup type detection, capability discovery, and `.pgpass` / profile matching):

```powershell
$env:PYTHONPATH = "."
pytest -v
```

---

## Safety Boundaries & Non-Goals

Use this tool only when the target database is offline or dedicated to the rebuild. Do not run it against live databases serving active application traffic.

Intentional non-goals and boundaries:

- **Windows operational focus:** PostgreSQL binary and service discovery target Windows hosts.
- **Operator-in-the-loop:** assumes an operator confirms target databases, drop/recreate actions, and post-restore checks interactively.
- **Not a fleet backup/PITR orchestrator:** does not manage retention catalogs, WAL archiving, or automated CI/CD restore pipelines.
- **Lightweight validation:** post-restore checks verify object counts and connectivity, not application-level data invariants.

See [`SECURITY.md`](SECURITY.md) for the security and operational-safety policy, and [`RELEASING.md`](RELEASING.md) for release verification steps.

---

## Portfolio Context

This repository complements related PostgreSQL operational tooling:

- `incident-response-runbook`: structured incident diagnosis, escalation, and communication
- `pg-incident-recovery`: multi-cluster Windows service recovery orchestration after crashes or reboots
- `windows-postgres-deployment-installer`: standardized PostgreSQL bootstrap on Windows hosts

---

## License

MIT (see [`LICENSE`](LICENSE)).
