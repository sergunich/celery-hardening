# Self-contained environment validation

Validated on **2026-11-17** without moving the working repository. No experiment
repository commit or remote was created, and nothing was published. Temporary Git
fixtures and upstream clone origins were used for setup validation. No production
source changes were made.
See [environment setup](README.md) for commands and lock responsibilities.

## Independent setup

An isolated copy outside any Celery checkout was created with no copied virtualenv.
The setup task created its own environment; the final setup also cloned Celery and
Kombu afresh from their upstream URLs into that copy's ignored `repos/` directory.
Tools came from mise; the already installed Redis binary was reused, not rebuilt.

- Python **3.13.9**, uv **0.11.15**, Redis **8.2.3**.
- Celery `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`.
- Kombu `703e96f22fb7a9c8da9a560b8b01f8766e654265`.
- **19 installed Python distributions** shared by all suites; Celery and Kombu
  imported from Git sources, not installed distributions.
- **242/242 Celery/Kombu source hashes** match the retained
  [historical source audit](../redis-visibility/redis-visibility-audit/manifest.json).
- The final worker summaries' root-relative harness/source hashes match current
  files. The temporary validation copy's new evidence was copied back without
  overwriting existing files.

The independent copy exposed four documentation links that still escaped to an
outer checkout; these were fixed. An inherited uv Python preference also caused
warnings; project configuration now selects mise's interpreter explicitly and
disables uv-managed Python downloads/preference. Final setup/checks have no such
warning.

Evidence: [initial fresh environment](portability-detached-setup.log),
[final fresh source setup](portability-fresh-setup.log),
[working-environment setup](portability-setup-validation.log),
[machine-readable validation](portability-validation.json).

## Unit and regression checks

**51/51 passed** both in the independent copy and the working repository:

| Checks | Count |
| --- | ---: |
| Source setup safety: local clone, detached pin, repeat setup, dirty/non-checkout/origin safeguards, external import rejection | 5 |
| Shared locks, import origins, mise task wiring, syntax, Markdown links/fragments, CLI help from unrelated cwd, relative provenance | 8 |
| Shutdown renderer and appendix preservation | 5 |
| Redis fault-proxy framing | 5 |
| Visibility observation wrappers | 5 |
| Visibility configuration wiring | 9 |
| Solo hardening | 14 |

Logs: [independent copy](portability-fresh-unit-validation.log),
[working repository](portability-final-unit-validation.log).

These checks do not start workers or Redis servers. Source-setup tests use
temporary local Git repositories and do not contact GitHub. All runner help,
including solo-requeue help, exits without running experiments.

## Real-worker portability checks

**30/30 final smoke cases passed** in the independent copy, using the common mise
task and fresh source checkouts. All four pools were exercised, including prefork
spawn and gevent CLI initialization.

| Suite | Cases | Evidence |
| --- | ---: | --- |
| Shutdown: warm late ACK in all pools | 4 | [summary](../shutdown/pool-shutdown-runs/20261117T134925.385141Z/summary.json) |
| Solo requeue controls | 2 | [summary](../shutdown/solo-requeue-runs/20261117T134944.007469Z/summary.json) |
| ETA due-time execution in all pools | 4 | [summary](../eta-prefetch/eta-prefetch-runs/20261117T134948.035671Z/summary.json) |
| Ordinary failure in all pools; actual prefork child-loss retry | 5 | [summary](../task-policy/task-policy-runs/20261117T135010.704699Z/summary.json) |
| Idle reconnect in all pools; failed ACK with successful-task deduplication | 5 | [summary](../redis-connection/redis-connection-runs/20261117T135031.774836Z/summary.json) |
| Broker-only visibility recovery, reserved-work expiry, result expiry | 3 | [summary](../redis-visibility/redis-visibility-runs/20261117T135102.008305Z/summary.json) |
| Solo hardening controls | 7 | [summary](../solo-redis-hardening/runs/20261117T135123.803237Z/summary.json) |

[Final smoke console](portability-fresh-smoke-validation.log);
[aggregate summary and per-suite logs](validation/20261117T134924.752484Z/summary.json).

No safety-deadline kills were required. Planned process death/crash injection is
part of the policy and visibility controls. Each runner owned and cleaned up its
private Redis/worker processes. An earlier 30-case smoke pass and one local warm
control are also retained, but are not counted as the final validation above.

This is portability validation, not a rerun of every historical behavioral matrix,
a Linux certification, a Redis persistence/failover test, or production-load proof.

## Historical integrity

Compared with experiment revision `6ba8398`:

- **3,333 of 3,359 existing tracked files remain byte-identical**, accounting for
  the archived requirements-lock rename.
- **25 existing files changed**: maintained code/docs and the ignore file.
- The obsolete setup shell script was removed; mise tasks replace it.
- All existing logs, JSON/JSONL evidence, audit/source manifests, and the old
  dependency lock remain byte-identical.
- The old requirements lock moved to
  [historical-requirements.lock](historical-requirements.lock); it is not active
  environment configuration.
- Historical reports still cite their original complete runs. No full report was
  regenerated from these smoke results.

Current commands, maintained links and provenance are repository-relative.
Historical artifacts retain their original locations/hashes as recorded, and
runtime traces/socket URLs can still contain absolute observation-time paths.
