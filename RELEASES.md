# Releases & Schema Compatibility

This file tracks the compatibility between pgwerk library releases and the
on-disk schema in your Postgres database. Use it to decide whether a given
library version can run safely against a given DB schema, and to plan
rolling upgrades across multiple processes (workers, API, dashboard,
producers).

For per-release behavioural changes, see [`CHANGELOG.md`](./CHANGELOG.md).

## How compatibility is enforced

Every pgwerk build declares three numbers in `WerkConfig`:

| Constant | Meaning |
|---|---|
| `schema_version` | The schema this build *writes* when it runs migrations |
| `min_compatible_db_version` | Oldest DB schema this build can read/write against |
| `max_compatible_db_version` | Newest DB schema this build tolerates (`None` = unbounded forward) |

On `Werk.connect()`, the live DB version is read from `_pgwerk_versions` and
validated against `[min, max]`. If the DB is too old, the build refuses to
start with `SchemaVersionMismatch` and a hint to run `werk migrate`. If the
DB is newer than `max`, it refuses with a hint to upgrade the application.

The check runs even when `auto_migrate=False`, so a producer-only or
read-only process (typically the API/dashboard pod) can connect without
owning schema, while still being protected from a stale binary.

## Compatibility matrix

| Library | `schema_version` | `min_compatible` | `max_compatible` | Notes |
|---|---|---|---|---|
| 0.1.12 | 6 | 6 | unbounded | Adds version-compat check at `connect()`; lazy repos; API runs with `auto_migrate=False` |
| 0.1.11 | 6 | 6 | unbounded | Adds `_pgwerk_schedules`; renames `jobs.cron_name` → `jobs.schedule_name`. Breaking against schema ≤ 5 |
| 0.1.10 | 5 | 5 | unbounded | Adds `worker.role` column for scheduler registration |
| 0.1.9  | 5 | 5 | unbounded | Default schema isolated to `pgwerk` (was `public`) |
| 0.1.8  | 4 | 4 | unbounded | Standalone `werk migrate` CLI, optional `auto_migrate=False` |
| 0.1.7  | 4 | 4 | unbounded | `failure_mode` column + dequeue/recovery indexes |
| ≤ 0.1.6 | 3 | 3 | unbounded | Heartbeat columns on `jobs` |

Older versions did not declare compat constants explicitly — `min` and
`max` are inferred from the migration history. From 0.1.12 onward, every
release ships explicit `min_compatible_db_version` and
`max_compatible_db_version` constants, and this file is updated alongside
the release.

## Upgrade flow

The recommended order for upgrading across pods:

1. **Run migrations once** — either let the worker pod boot with
   `auto_migrate=True` (default), or run `werk migrate` in a Kubernetes
   `Job` / deploy hook before rolling out new pods.
2. **Roll workers** — they migrate-on-boot, but at this point migrations
   are already a no-op.
3. **Roll API/dashboard pods** — these run with `auto_migrate=False` and
   only verify the version. They will fail fast if step 1 was skipped.
4. **Roll producers** (e.g. external services that enqueue) — they don't
   need `connect()` and can hot-load the new library version at their own
   pace, as long as the running schema is within their compat range.

## When to bump `min_compatible_db_version`

Bump `min` whenever a migration is **not safely backward-compatible** with
the previous-release code. Concrete examples:

- Renaming a column (e.g. v6's `cron_name` → `schedule_name`)
- Dropping a column the previous release reads or writes
- Adding `NOT NULL` to an existing column without a default

When a migration *is* backward-compatible (additive nullable column, new
table, new index), bump `schema_version` but leave `min` where it is.
This is the case where `min < schema_version` pays off — older API pods
can keep running through a rolling deploy because they don't depend on
the new column or table.
