# Celery Redis experiments

Main reports and setup recommendations stay at this directory's root.
Suites own their scripts, probes, logs, runs, and supporting documents (including the shutdown appendix).

| Suite directory | Report / documentation | Contents |
| --- | --- | --- |
| [shutdown/](shutdown/) | [Shutdown comparison](shutdown.md), [maintained appendix](shutdown/shutdown-appendix.md) | Four-pool shutdown runner/probe, renderer/tests, native confirmation, solo requeue verification, both run archives |
| [eta-prefetch/](eta-prefetch/) | [ETA/prefetch](eta-prefetch.md) | ETA probe/runner, runs and console logs |
| [task-policy/](task-policy/) | [Task policies](task-policy.md) | Policy probe/runner, runs and console logs |
| [redis-connection/](redis-connection/) | [Redis connections](redis-connection.md) | Connection probe/runner, fault proxy/tests, runs and console logs |
| [redis-visibility/](redis-visibility/) | [Redis visibility](redis-visibility.md) | Visibility probe/runner, observation/wiring tests, runs, source/history audit, console logs |
| [environment/](environment/) | This index | Shared environment setup/lock, relocation map, validation notes and regression checks |

Setup recommendations: [comparison](compare-redis.md),
[solo](solo-redis.md), [prefork](prefork-redis.md),
[threads](threads-redis.md), [gevent](gevent-redis.md).

## Environment and cross-suite dependencies

The existing **`.dev/.venv` stays in place**: relocating a virtual environment can
break its absolute interpreter/script paths. From the Celery checkout root:

```bash
bash .dev/environment/setup-shutdown-env.sh
```

This uses uv and mise Redis, not an existing/shared Redis service. The lock is
shared by every suite; no per-suite environments or production code changes are needed.

The shutdown suite remains the owner of the shared private-Redis/process helpers
and `shutdown_probe` instrumentation. ETA, policy, and connection workers reuse that
probe; the visibility probe is independent. Connection/visibility runners also reuse
policy cleanup helpers. Runners resolve these paths from their own locations and
supply explicit worker `PYTHONPATH` entries, including for prefork spawn. They do not
require the caller's current directory to be the checkout root.

## Run experiments

Commands below assume the checkout root. Runners write **new timestamped artifacts
inside their suite directory**, without overwriting maintained reports or old runs.
The full suites start real workers/private Redis and include intentional faults,
signals, and process-loss cases. Use `--case` for targeted verification.

```bash
.dev/.venv/bin/python .dev/shutdown/verify-pool-shutdown.py --case warm-late
.dev/.venv/bin/python .dev/shutdown/verify-solo-requeue.py
.dev/.venv/bin/python .dev/eta-prefetch/verify-eta-prefetch.py --case due-solo-late
.dev/.venv/bin/python .dev/task-policy/verify-task-policies.py --case failure-solo-ack
.dev/.venv/bin/python .dev/redis-connection/verify-redis-connections.py --case idle-reconnect-solo
.dev/.venv/bin/python .dev/redis-visibility/verify-redis-visibility.py --case config-B
```

The solo-requeue verifier always runs its two cases; it has no `--help`/`--case`
parser. Other experiment runners expose their available cases with `--help`.

## Tests and shutdown rendering

```bash
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/environment/test-suite-layout.py
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/shutdown/test-render-shutdown-report.py
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/redis-connection/test-redis-fault-proxy.py
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/redis-visibility/test-redis-visibility-probe.py
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/redis-visibility/test-visibility-config-wiring.py

.dev/.venv/bin/python .dev/shutdown/render-shutdown-report.py \
  .dev/shutdown/pool-shutdown-runs/20261113T081413.844718Z/summary.json \
  --previous .dev/shutdown/pool-shutdown-runs/20261113T080741.135372Z/summary.json
```

Only the renderer writes `shutdown.md`; it requires a complete 96-case
summary and includes `shutdown/shutdown-appendix.md`, rebasing its links. Edit the appendix source,
not its rendered copy. Partial smoke runs must not replace full-suite report data.
Other reports are maintained manually.

## Historical artifact integrity

Raw logs, run JSON/configuration, source-hash manifests, audit snapshots, and the
pinned dependency lock were **moved without rewriting their contents**. Their
recorded absolute paths, traceback paths, source-hash keys, and the lock's original
generation-command comment describe the original execution, not commands to use now.
Old socket/process identifiers are historical too, not resources to reconnect to.

Use [environment/layout-migration.json](environment/layout-migration.json) to map
an old `.dev/`-relative first path component to its new location. For example,
`redis-connection-runs/<timestamp>/summary.json` is now
`redis-connection/redis-connection-runs/<timestamp>/summary.json`. Celery source
paths and `.dev/.venv` did not move. Changed runner hashes in new results are expected;
old manifests must not be rewritten to pretend they captured the relocated scripts.

All maintained report links, runnable examples, renderer output, source-hash input
paths, and cross-suite imports use the new layout. There are no old-root compatibility
copies/symlinks. See [layout validation](environment/layout-validation.md) for the relocation
checks and targeted runtime evidence, distinct from the full historical matrices.
