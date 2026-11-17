# Shared experiment environment

All commands and paths are relative to the experiment repository root. No enclosing
Celery checkout, global Python packages, or shared Redis service is required.

## Setup

Use mise 2026.8.4 (validated here) or a compatible newer version, Git, and a C build
toolchain for Redis (Xcode command-line tools on macOS; suitable build packages on
Linux). After reviewing the configuration:

```bash
mise trust
mise install --locked
mise run setup
mise run check
mise run test
```

Setup does not start Redis or workers. It checks out locked upstream sources and
syncs one shared `.venv/` without updating dependency resolution. New environments
use mise's pinned interpreter explicitly, not a separately cached uv-managed build.
An incompatible existing environment is recreated in place by uv; it is not moved. Worker tasks
perform a read-only preflight; they do not automatically sync dependencies or
replace source checkouts. Run setup explicitly when the environment is absent.

## Pins and locks

| File | Responsibility |
| --- | --- |
| [mise.toml](../mise.toml) | Exact Python 3.13.9, uv 0.11.15, Redis 8.2.3; shared task definitions and environment |
| [mise.lock](../mise.lock) | Tool download URLs for macOS ARM64 and Linux x64; Python/uv artifact checksums |
| [.python-version](../.python-version) | Exact Python version for uv and other Python-aware tools |
| [pyproject.toml](../pyproject.toml) | One non-package uv project; exact runtime dependency versions |
| [uv.lock](../uv.lock) | Full dependency resolution, platform markers, artifact hashes |
| [repos.lock.toml](../repos.lock.toml) | Upstream URLs, full immutable Celery/Kombu revisions and expected versions |

The Redis mise backend records its exact release URL but does not emit a checksum
in the generated lock. Redis is built natively: its compiler, system libraries and
OS are prerequisites, not a hermetic build environment. Linux download locks are
provided; the behavioral evidence remains scoped to the recorded macOS platform.

The former [pip requirements lock](historical-requirements.lock) is archival
evidence only. Neither setup nor tests use it to resolve dependencies.

## Source-only Celery and Kombu

- `repos/celery/`: Celery 5.7.0 at
  `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`.
- `repos/kombu/`: Kombu 5.7.0 at
  `703e96f22fb7a9c8da9a560b8b01f8766e654265`.

The `repos/` directory is Git-ignored. Setup uses detached checkouts and refuses
dirty sources, unexpected origins, symlinks, or non-checkout directories rather
than deleting/resetting them. It can switch a clean checkout when a pin is
deliberately updated.

Mise supplies `PYTHONPATH` with these two checkouts and the experiment root, and
sets `UV_PROJECT_ENVIRONMENT` to this repository's `.venv/`.
Runners independently configure the same import roots, including in child-worker
environments, so prefork spawn does not fall back to another installation.
Celery and Kombu are deliberately absent from the uv distribution lock; their
runtime dependencies are pinned there instead. Preflight verifies upstream
requirement compatibility, actual import origins, source revisions and versions,
and rejects separately installed Celery/Kombu distributions.

No Celery, Kombu, Billiard, or Redis implementation is patched.

## Tasks and shared utilities

`mise tasks` lists the available commands:

- `setup`: clone/check out sources, sync the environment, verify pins.
- `sources` / `sync`: run the two explicit setup steps separately.
- `check`: detect environment/source/lock drift without installing anything.
- `test`: all unit/regression checks; no worker or Redis server starts.
- `smoke`: 30 focused real-worker cases spanning all seven runners and four pools.
- `shutdown`, `solo-requeue`, `eta-prefetch`, `task-policy`,
  `redis-connection`, `redis-visibility`, `solo-hardening`: suite entry points.
- `render:shutdown`: render only from a complete 96-case shutdown summary;
  **overwrites the maintained shutdown report**.

Tasks forward arguments, including `--help`. For example:

```bash
mise run eta-prefetch --case cap-three --case cap-release
mise run shutdown --pool prefork --case warm-late
mise run solo-requeue --help
```

[manage.py](manage.py) implements source preparation, preflight, test discovery and
the focused smoke matrix. [runtime.py](runtime.py) centralizes root/source paths,
dependency versions, source/harness provenance, and private Redis lifecycle.
Individual suite runners retain their experimental logic and observers; common
process cleanup is also reused from the existing shutdown/task-policy harnesses.

The private Redis fixture disables TCP and persistence, owns a Unix socket at the
repository root, and cleans up only its own process. Keep the repository path
short enough for the platform's Unix-socket limit. Fault proxies use private
loopback ports where the experiment requires TCP faults.

## Updating dependencies deliberately

1. Preserve old evidence. Do not regenerate historical manifests or reports from
   partial smoke runs.
2. Update exact tool pins in `mise.toml`. Keep Python's pin consistent with
   `.python-version` and `requires-python` in `pyproject.toml`.
3. Update Git revisions and expected versions in `repos.lock.toml`. Review the
   pinned upstream requirement files; change the shared Python dependencies
   explicitly if needed.
4. Regenerate locks intentionally, then inspect the diff:
   `mise lock --platform macos-arm64,linux-x64` and `mise exec -- uv lock`.
5. Run `mise install --locked`, `mise run setup`, `mise run test`, then relevant
   worker cases. Passing old bug reproductions is not proof that an upgrade is safe.

Normal setup uses `--locked`; it fails on stale locks rather than silently updating
them. A newer tool/source combination needs new measured evidence.

## Relocation and evidence

Paths in commands, links, source pins and provenance are repository-relative.
Mise sets each task's cwd to the repository root. Source discovery derives that
root from the shared utility's location, never from its parent directory.

When moving the repository later, recreate `.venv/`; its executables embed
absolute interpreter paths. The ignored source checkouts can be recreated from
the source lock. Nothing in this migration creates a remote or publishes GitHub
content.

Historical artifacts retain original paths and hashes. Runtime logs can contain
absolute tracebacks, interpreter paths and Unix-socket URLs; those describe a run,
not a dependency on the repository's former location. See
[portability validation](portability-validation.md).
