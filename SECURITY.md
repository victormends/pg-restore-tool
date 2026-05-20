# Security And Operational Safety

This repository is a public PostgreSQL restore utility for local or operator-controlled Windows recovery scenarios. It is not a managed backup service, disaster-recovery platform, or zero-risk production restore system.

## Supported Use

Use this project only in environments where you are authorized to connect to PostgreSQL, inspect backup files, create safety backups, and drop or recreate the target database.

Before using it on a real database:

- Confirm the target database is offline or effectively offline.
- Confirm the selected backup is the intended recovery source.
- Run with `--dry-run` first.
- Use the `safe` or `fast` restore profile unless the PostgreSQL instance is dedicated to the restore.
- Keep an operator log of destructive decisions such as dropping a database or continuing without a safety backup.

## Safety Boundaries

The tool can execute destructive restore steps when the operator confirms them. In particular, it may terminate sessions, drop the target database, recreate it, and restore a selected backup.

The tool is intentionally not intended for:

- live in-place restore under active application traffic
- multi-environment disaster recovery orchestration
- point-in-time recovery management
- cloud backup retention policy enforcement
- unattended production restores without operator review

The `unsafe` profile may change cluster-wide durability settings temporarily. Use it only when the instance is dedicated to the restore and the operational risk is acceptable.

## Reporting Security Issues

If you find a security issue in the public code or documentation, open a GitHub issue with enough detail to reproduce the concern without including secrets, customer data, hostnames, passwords, connection strings, or production log excerpts.

For sensitive reports, contact the maintainer privately through the contact information on the GitHub profile instead of posting operational details publicly.

## Data Handling

Do not commit real backup files, restore logs, database names from client environments, credentials, hostnames, connection strings, `.pgpass` entries, or screenshots from production systems.
