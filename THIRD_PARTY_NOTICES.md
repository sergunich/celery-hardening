# Third-party notices

Original code, reports, guides, and other original material in Celery Hardening
are licensed under the [MIT License](LICENSE). That grant does not relicense
third-party material reproduced in quotations, source excerpts, patches,
tracebacks, or research snapshots. Such material retains its applicable upstream
copyright notices and licenses.

The source checkouts under ignored `repos/` are fetched separately and are not
part of this repository's tracked distribution. Some tracked evidence does contain
upstream material, as identified below.

## Celery

- Project: [celery/celery](https://github.com/celery/celery).
- Software license: **BSD-3-Clause**.
- Recorded source revision: `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`.
- [Complete upstream license and copyright notices](licenses/Celery-LICENSE.txt),
  copied unchanged from that revision's
  [LICENSE](https://github.com/celery/celery/blob/6fcdbf904d410749ad7676d8ff6b1b25d6e19675/LICENSE).

Retained material includes source and documentation diffs in
[git-evidence.txt](redis-visibility/redis-visibility-audit/git-evidence.txt),
API snapshots in the [visibility audit](redis-visibility/redis-visibility-audit/),
and attributed source/documentation excerpts in the reports. The audit records
the older upstream commits used for historical comparisons; any notices attached
to those excerpts continue to apply. Source lines appearing in recorded tracebacks
also remain upstream material, not newly MIT-licensed code.

### Celery documentation

Celery's upstream license identifies its documentation portion as
**Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)**.
Reproduced or adapted Celery documentation retains those terms where applicable;
it is not relicensed by this repository's MIT license.

- [License summary](https://creativecommons.org/licenses/by-sa/4.0/).
- [Legal text](https://creativecommons.org/licenses/by-sa/4.0/legalcode).
- Attribution: the Celery project and the authors credited in the linked upstream
  documentation, commits, and retained snapshots.

Examples include quotations from the
[Redis guide](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html)
in [the visibility report](redis-visibility.md), and the
[Workers Guide](https://docs.celeryq.dev/en/stable/userguide/workers.html)
in [the connection report](redis-connection.md). Sources and historical revisions
are identified alongside the excerpts or in the associated audit.

The snapshots preserve the captured text and diffs; reports add commentary and may
excerpt or format quoted passages. Our surrounding original analysis is MIT-licensed.
This does not place all documentation in this repository under CC BY-SA.

## Kombu

- Project: [celery/kombu](https://github.com/celery/kombu).
- Software license: **BSD-3-Clause**.
- Recorded released-source revision: `703e96f22fb7a9c8da9a560b8b01f8766e654265`.
- [Complete upstream license and copyright notices](licenses/Kombu-LICENSE.txt),
  copied unchanged from that revision's
  [LICENSE](https://github.com/celery/kombu/blob/703e96f22fb7a9c8da9a560b8b01f8766e654265/LICENSE).

The retained [PR #2831 file snapshot](solo-redis-hardening/kombu-pr-2831-files.json)
contains proposed source, test, and documentation changes, not original Celery
Hardening code. Its [metadata](solo-redis-hardening/kombu-pr-2831.json) identifies
the contributors and [upstream pull request](https://github.com/celery/kombu/pull/2831).
The recorded proposal head is `7515088f6f52717c62dcdd97c62a4d3e0f7b4ee4`;
it is distinct from the released-source revision above.

Kombu excerpts and source lines in tracebacks likewise retain upstream rights.

## Issue discussions and pull-request metadata

Issue descriptions, comments, and pull-request discussions in the visibility
audit and PR snapshots are attributed to their authors through the retained
author fields, timestamps, and upstream URLs. They are third-party research
material, not original MIT-licensed writing from this project.

Public availability alone is not a blanket license. Do not assume that a
repository's software license automatically covers every discussion comment.
This project's MIT license grants no additional rights over that content;
consult the original source and applicable permissions before reusing it.

## Other dependencies and excerpts

Python packages and tools installed by uv or mise remain under their respective
licenses; pinning or executing them does not relicense them. They are not vendored
in this repository. Any third-party source lines or notices retained in logs
remain subject to their original terms.

Preserve applicable upstream notices and attribution when redistributing retained
third-party material. Inclusion here does not imply upstream endorsement of
Celery Hardening.
