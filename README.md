# PG Restore Tool

Windows-first PostgreSQL restore utility for a narrow operational scenario: rebuilding a local or operator-controlled database quickly after workstation failure, reinstall, or environment corruption.

This is not positioned as a production-grade managed restore platform. It is a niche recovery tool built around a real support workflow where the target database is already offline, the operator is present, and the fastest safe-enough rebuild path matters more than broad platform coverage.

## Project Framing

This repository is best understood as a support engineering artifact, not a general-purpose backup product.

It shows how I think about:

- operator-guided recovery instead of blind automation
- explicit tradeoffs between speed and safety
- Windows-based PostgreSQL support realities
- restore workflows that fail loudly on destructive steps instead of silently continuing

It is intentionally scoped to a narrower problem than cloud backup tooling, PITR orchestration, or multi-environment disaster recovery.

## Current Status

This version is a publishable niche tool with several hardening improvements already applied:

- profile-based restore tuning (`safe`, `fast`, `unsafe`)
- subprocess-local credential handling in the main restore path
- safer default restore acceleration via session-level settings
- explicit failure handling for `DROP DATABASE` and `CREATE DATABASE`
- archive validation before non-SQL restores

It still has important non-goals and limitations:

- Windows-first assumptions remain throughout the repo
- post-restore validation is intentionally lightweight
- it is not a managed cloud restore system
- it is not a zero-risk production restore tool

If you are evaluating this repo, the right question is not "is this a universal PostgreSQL restore solution?" The right question is "does this encode a real, narrow recovery workflow honestly and pragmatically?"

## Why This Tool Exists

The original idea behind this tool is simple:

- a local PC or workstation fails
- the local PostgreSQL setup needs to be rebuilt
- the target database is effectively offline already
- there is no need to preserve an in-use running database during the restore
- speed matters more than conservative defaults

In that scenario, the operator usually needs a fast, repeatable workflow to:

- find a backup file
- connect to the local PostgreSQL instance
- drop and recreate the damaged database if necessary
- restore it as quickly as possible
- confirm that the database is back online

That is the problem this repository is trying to solve.

## Design Assumptions

This project assumes all of the following are true:

- the restore is performed on Windows
- PostgreSQL is running locally or in a local controlled environment
- the database is not serving active application traffic during restore
- the operator is present and making the decisions interactively
- the target database may be dropped and recreated from scratch
- the backup being restored is the source of truth for recovery

If those assumptions are not true, this tool is probably the wrong tool.

## Why Windows-Only

This tool was designed around the environment where it was actually used: Windows machines.

That decision is intentional, not accidental.

Reasons:

- the real deployment environment was Windows
- PostgreSQL detection was built around Windows paths and Windows service discovery
- operator workflow was based on local Windows support and recovery
- reducing cross-platform abstraction kept the tool simpler and faster to operate

The goal here was not broad portability. The goal was to make the Windows recovery path practical.

## Restore Profiles

The tool now uses restore profiles instead of one always-aggressive turbo mode.

Available profiles:

- `safe`: no durability changes, no session tuning
- `fast`: safer default, uses session-level restore tuning only
- `unsafe`: keeps the old cluster-wide durability tradeoffs and requires explicit confirmation

The default `fast` profile avoids changing cluster-wide durability settings. Instead, it applies per-process restore settings such as:

- `synchronous_commit = off`
- higher `maintenance_work_mem`

That keeps restore performance improvements local to the restore session and avoids weakening unrelated databases in the same cluster.

`unsafe` mode still exists for isolated recovery cases, but it should be used only when the PostgreSQL instance is dedicated to the restore operation.

## Safety Model

This tool is not "safe" in the conservative enterprise sense.

Its safety model is narrower:

- fail explicitly on destructive database operations
- validate archive backups before restoring them
- stop `.sql` restores on the first meaningful SQL error
- always attempt to restore unsafe profile settings in a `finally` block
- provide lightweight post-restore validation

This is meant to reduce avoidable operator mistakes inside a destructive recovery workflow. It is not meant to make destructive recovery non-destructive.

## Intended Workflow

Typical use cases:

- reinstalling PostgreSQL on a Windows PC
- recovering a broken local application database
- rebuilding a local environment after machine failure
- restoring a known-good backup into a newly recreated database

Typical flow:

1. Detect PostgreSQL binaries and local instances.
2. Locate a backup file.
3. Connect using operator-provided credentials.
4. Optionally create a safety backup of the current database.
5. Drop and recreate the target database if needed.
6. Prepare the selected restore profile.
7. Restore the selected backup.
8. Disable turbo mode.
9. Run lightweight validation.

## Non-Goals

This project is intentionally not trying to be:

- a managed-cloud restore orchestrator
- a multi-platform PostgreSQL recovery framework
- a zero-risk production migration tool
- a full backup catalog and retention system
- a continuous automation/CI restore pipeline

Those are different products with different tradeoffs.

## Technical Choices In This Version

The current version includes several corrections to make the tool more reliable without changing its core philosophy:

- `DROP DATABASE` and `CREATE DATABASE` fail explicitly instead of being ignored
- turbo mode is always disabled in a `finally` block
- `.sql` restores use `ON_ERROR_STOP=1`
- `pg_restore` no longer uses incorrect restore format flags
- the main restore flow now dispatches `sql`, `custom`, `tar`, and `directory` types correctly
- administrative SQL quoting was corrected
- broken sequence realignment was removed from the main flow
- connection checks now use `PGSSLMODE` correctly
- pre-restore backup command construction was corrected
- the operator-facing interface was translated fully to English

## Operational Warning

Use this tool only when the target database is offline or effectively offline.

Do not use this workflow when:

- applications are actively connected
- the database must remain durable throughout the operation
- server-wide settings cannot be changed
- the environment requires conservative production-grade recovery controls

This tool is designed for fast rebuild and recovery on Windows, not for live in-place restore under load.

## Portfolio Context

This repo complements the rest of the PostgreSQL support tooling in my public profile:

- `incident-response-runbook`: how I structure diagnosis, escalation, and communication during incidents
- `pg-incident-recovery`: how I automate safe recovery on multi-cluster Windows hosts after crashes or reboots
- `windows-postgres-deployment-installer`: how I standardize PostgreSQL deployment on Windows while keeping a human operator in the loop

`pg_restore_tool` sits lower in the stack than those repos: it focuses specifically on the restore path for a single target database in a controlled rebuild scenario.
