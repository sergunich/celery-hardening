# Experiment layout relocation validation

This validates the directory reorganization, not a new full behavioral matrix.
The maintained reports still cite their original complete runs. No Celery, Kombu,
Billiard, or Redis production code/configuration was changed.

See [README.md](README.md) for the layout, reproduction commands, and historical-path
policy. All maintained reports and recommendation guides remain at `.dev/` root.

## Files and historical integrity

Compared against nested `.dev` Git revision
`aab808d6d5159d42c847e486f7c8f49b192550ac` before the reorganization:

- **3,174 tracked files relocated**, grouped by experiment or shared environment.
- **3,169 of 3,186 existing tracked files remain byte-identical** at their new/current
  locations. The 17 edited existing files are ten scripts and seven Markdown reports.
- Historical console logs, run/config/result JSON, audit evidence, dependency lock,
  and probe/fault-injector implementations were not rewritten.
- [The path map](environment/layout-migration.json) records old-to-new top-level
  locations. Old absolute execution paths and hash-manifest keys remain historical
  evidence; current scripts, report links, and new artifact paths use the new layout.

Git blob hashes were compared for every existing tracked file after mapping paths;
all destinations existed, and no raw artifact or lock content changed.

## Unit and layout checks

**28/28 tests passed**: the 23 existing tests plus five layout regression tests.
All were invoked from `/tmp`, not the checkout root.

| Tests | Count | Location |
| --- | --- | --- |
| Shutdown appendix preservation | 4 | [test-render-shutdown-report.py](shutdown/test-render-shutdown-report.py) |
| Fault-proxy RESP framing | 5 | [test-redis-fault-proxy.py](redis-connection/test-redis-fault-proxy.py) |
| Visibility observation wrappers | 5 | [test-redis-visibility-probe.py](redis-visibility/test-redis-visibility-probe.py) |
| Visibility configuration wiring | 9 | [test-visibility-config-wiring.py](redis-visibility/test-visibility-config-wiring.py) |
| Layout regression | 5 | [test-suite-layout.py](environment/test-suite-layout.py) |

Logs: [shutdown](shutdown/layout-unit-tests.log),
[proxy](redis-connection/layout-unit-tests.log),
[visibility wrappers](redis-visibility/probe-layout-unit-tests.log),
[wiring](redis-visibility/wiring-layout-unit-tests.log),
[layout](environment/layout-unit-tests.log).

The layout regression test additionally checks:

- Migration targets exist without old-root compatibility copies.
- All current suite Python scripts parse without importing side-effecting probes.
- Root Markdown file links and explicit `.dev/` paths resolve.
- Five runners and the renderer support `--help` from an unrelated working directory
  without an inherited `PYTHONPATH`.
- The setup script resolves the Celery root and shared lock from another working
  directory. Stubbed uv/mise commands prevent installations or dependency changes;
  its final import check uses the real existing environment and checkout.

The environment setup script also passed `bash -n`. The real shutdown renderer was
run against the original 96-case summary and preceding summary; its output remained
byte-identical to the path-updated report and still embeds the maintained appendix.

## Targeted real-worker checks

**23/23 cases passed**, invoked from `/tmp` using absolute script paths and the
existing `.dev/.venv`. Each runner created and cleaned up its own private Redis;
no shared Redis, Docker, or production services were used. Prefork spawn and gevent
CLI early initialization were exercised, not just module syntax/imports.

| Suite | Checks | New summary | Console |
| --- | --- | --- | --- |
| Shutdown | Warm late-ACK task in all four pools: 4 cases | [summary](shutdown/pool-shutdown-runs/20261115T100605.803741Z/summary.json) | [log](shutdown/layout-validation.log) |
| Solo requeue | Early/late-ACK workaround: 2 cases | [summary](shutdown/solo-requeue-runs/20261115T100605.836861Z/summary.json) | [log](shutdown/requeue-layout-validation.log) |
| ETA/prefetch | Due late-ACK ETA task in all four pools: 4 cases | [summary](eta-prefetch/eta-prefetch-runs/20261115T100605.792666Z/summary.json) | [log](eta-prefetch/layout-validation.log) |
| Task policies | Ordinary failure in all pools; actual prefork child-loss retry: 5 cases | [summary](task-policy/task-policy-runs/20261115T100605.792191Z/summary.json) | [log](task-policy/layout-validation.log) |
| Connections | Idle reconnection in all pools; failed ACK with deduplication: 5 cases | [summary](redis-connection/redis-connection-runs/20261115T100605.874240Z/summary.json) | [log](redis-connection/layout-validation.log) |
| Visibility | B-only crash recovery, prefork reserved expiry, independent result expiry: 3 cases | [summary](redis-visibility/redis-visibility-runs/20261115T100605.836131Z/summary.json) | [log](redis-visibility/layout-validation.log) |

The visibility crash case intentionally killed its original worker; the policy case
intentionally lost a prefork child. No safety-deadline kills were needed. These runs
check relocated imports, subprocess environments, private Redis/proxy helpers,
source-hash inputs, cleanup, and suite-local artifact creation. The full historical
96/31/47/47/27-case matrices were not rerun for this path-only reorganization.
