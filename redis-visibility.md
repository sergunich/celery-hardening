# Redis visibility timeout: configuration, recovery, and solo execution

> **27/27 selected real-worker cases passed**, including reproduced duplicate
> execution. PASS means the asserted behavior was observed, not that the tested
> configuration is safe. Five observation-wrapper tests also pass.
>
> Companion reports: [shutdown](shutdown.md),
> [ETA/prefetch](eta-prefetch.md), [task policies](task-policy.md),
> and [Redis connections](redis-connection.md).
>
> This report and its appendix are **maintained text**. The runner writes only
> timestamped raw artifacts and never regenerates this explanation.

## 1. Direct answer: these are not three effective Redis visibility timers

The [Redis guide](https://docs.celeryqen/stable/getting-started/backends-and-brokers/redis.html#id3)
and this checkout's `repos/celery/docs/getting-started/backends-and-brokers/redis.rst` show:

```python
app.conf.broker_transport_options = {'visibility_timeout': 43200}
app.conf.result_backend_transport_options = {'visibility_timeout': 43200}
app.conf.visibility_timeout = 43200
```

The local guide says all three are required. **That requirement does not match
this checkout's Redis broker + Redis result-backend implementation.**

| Setting | Role in this tested stack |
| --- | --- |
| `broker_transport_options['visibility_timeout']` (**B**) | **Effective.** Passed to the Kombu Redis channel, then used by its unacked-message recovery scanner. Defaults to 3600 s when omitted. |
| `result_backend_transport_options['visibility_timeout']` (**R**) | Stored in backend options, but **not consumed as a visibility timeout by the Redis result backend**. Does not change broker recovery or result retention. |
| `app.conf.visibility_timeout` (**T**) | Accepted as a configuration key, but **not read by the checked Redis broker/backend paths**. Does not override B or supply its missing value. |

We tested every presence/absence combination and conflicting values. Setting
**B alone works**. Setting R, T, or both without B leaves the channel at **3600**.
When B conflicts with R/T, **B determines recovery**, not the minimum of the three.

Why do those configuration locations exist?

- `broker_transport_options` is the broker transport's options container.
- `result_backend_transport_options` is a separate backend options container.
  Redis supports options there such as result-key prefixes, retry policy, and
  chord ordering; that does not make every broker option meaningful there.
- Celery accepts additional configuration keys. Acceptance of `visibility_timeout`
  at the top level does not establish that an implementation reads it.
- Redis results use key/value storage and notifications, not the broker's
  BRPOP/unacked delivery mechanism. There is no matching result-delivery lease
  controlled by R in this implementation.

This establishes a documentation/implementation mismatch. A subsequent deep
source/history audit traced the recipe to an issue-comment workaround copied into
the documentation; see **appendices A5–A7** for the source paths, dated commits,
other transports/backends, and indirect effects through application configuration.
The conclusion concerns **independent settings in the built-in implementation**,
not every historical version, aliased options dictionary, or custom extension.

For this stack, configure the broker option while preserving unrelated options:

```python
app.conf.broker_transport_options = {
    **app.conf.broker_transport_options,
    'visibility_timeout': 43200,
}
```

**43200 s is 12 hours, not a recommendation.** It also lengthens recovery of
abandoned deliveries. All workers scanning the same broker namespace need a
compatible setting; setting it on only the publisher is insufficient.

## 2. What the timeout actually measures

Redis broker delivery is emulated using lists, a hash, and a sorted set:

```text
Message waits in Redis queue list                       ← not yet unacked
  → worker receives popped message
  → Kombu records delivery tag/message in unacked hash
  → Kombu records receipt time in unacked_index
  → ETA wait / pool wait / execution / delayed ACK
  → successful ACK removes the unacked entries
```

A recovery scanner compares the recorded receipt timestamp with:

```text
current wall-clock time - this scanner's visibility_timeout
```

Eligible records can be removed from the unacked structures and restored to their
queue. **The timeout is an eligibility threshold, not an exact redelivery timer.**
A live consumer must run the scan, obtain its mutex, restore the message, and a
consumer must then have capacity/credit to receive it.

Consequences:

- Time starts with delivery registration, **not task-body start**.
- Waiting in a Redis queue before receipt is not part of this unacked interval.
- ETA waiting and prefetched-but-unaccepted work can expire before execution.
- The timestamp is not periodically renewed because the task is still alive.
  Worker-monitoring heartbeats do not extend it.
- Expiry does not terminate the original task, cancel its local ETA timer, or
  make its result disappear.
- An ACK after the visibility age has elapsed can still settle a record if a
  scanner has not restored it. This is not a broker-enforced ACK rejection deadline.
- Once restored and received again, the delivery is registered with a new receipt
  timestamp. The task ID, ETA, and retry count are not rewritten as a task retry.

## 3. Experiment selection: what is new and what we reuse

| Area | New cases | Rationale |
| --- | ---: | --- |
| Configuration presence and conflicting values | 10 | All eight B/R/T combinations, plus B=12/R=T=2 and B=2/R=T=12; tests the documentation directly |
| Publisher-only short value | 1 | Distinguishes a producer's configuration from an active recovery scanner |
| No consumer after crash | 1 | Distinguishes eligibility from Redis-native expiration/redelivery |
| Scan cadence | 2 | Default cadence and explicit throttling, compared with accelerated B-only recovery |
| Solo alone, early/late ACK | 2 | Checks whether an expired record is restored while solo executes inline |
| Mixed workers, queues, prefixes, and early ACK | 5 | Tests shared recovery scope, overlapping vs sequential duplicates, namespace isolation, and a prefork early-ACK control |
| Solo ETA with short/long visibility and deduplication | 4 | Waiting does not block the solo loop; reconnect results cannot establish visibility-scan timer behavior |
| Reserved task behind active prefork task | 1 | Makes pool-wait time part of the measured unacked interval |
| Result expiration | 1 | Separates result TTL from all three visibility-looking settings |
| **Total** | **27** | |

### Prior measurements reused rather than repeated

- The shutdown suite establishes reachable-cleanup restoration of waiting
  messages even with a long visibility timeout, and that SIGKILL skips cleanup.
  We do not repeat every signal, ACK flag, and pool combination here.
- The ETA suite establishes that waiting ETA messages are unacked even with early
  ACK, occupy timer/memory rather than execution slots, and are not generally
  bounded by the ordinary prefetch window.
- The task-policy suite distinguishes ACK, reject/discard, raw requeue, and task
  retry. A settled/discarded message is not waiting for visibility recovery.
- Redis connection cases `visibility-overlap-prefork` and
  `visibility-solo-other-worker` already demonstrate overlapping bodies despite
  completed-task deduplication. This suite adds configuration conflicts and
  different-queue/prefix scope rather than repeating those exact cases.
- Connection cases `ack-before-commit-dedup`, `ack-reply-lost`, and
  `delivery-reply-lost` establish that failed ACK observation and recoverable
  broker state are not equivalent. We do not inject those faults again.

### Safe source-backed extrapolations, not additional measurements

- B/R/T option resolution is shared transport/backend code. The same wiring
  applies to prefork, threads, and gevent using this stack; an additional 30-case
  pool-by-configuration cross-product would not test a distinct mechanism.
- Shared unacked-record scope does not depend on the executor pool. Actual scan
  progress does: prefork/threads can keep their controller loop running, solo
  blocks it, and non-yielding gevent can starve its loop.
- Increasing B from 12 to 43200 changes the threshold, not the underlying
  mechanism. We did not wait 12 hours or the default 3600 s.
- Different Redis database numbers isolate unacked structures; this suite tests
  isolation by broker key prefix, not another database-isolation matrix.
- Native gevent scanning uses the synchronous drain-events path. Its exact scan
  cadence is **not** inferred from the async-loop timing measurements below.
- Per-task ACK overrides, disabled ACK emulation, custom unacked-key names,
  rate-limit combinations, clock jumps, and heavy-load scan limits are not new
  runtime cases here. Nor are duplicate counts under `worker_eta_task_limit` or
  `worker_disable_prefetch` inferred from the baseline ETA/prefetch results;
  those interactions would need targeted follow-up tests.

## 4. Environment, controls, and evidence

Celery **5.7.0**, checkout `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`;
Kombu **5.7.0**, redis-py **8.1.0**, Redis server **8.2.3**, Billiard **4.3.2**,
CPython **3.13.9**, macOS **15.8.1 ARM64**. Installed gevent **26.9.0** and greenlet
**3.5.6** are recorded for environment consistency; no new gevent cases run here.
Version numbers alone are insufficient: the summary records source hashes.

- Real Celery CLI workers; solo unless a case explicitly names prefork.
- A dedicated mise-managed Redis process, reachable only through its private
  Unix socket. Broker DB 0, result DB 1. No shared Redis is flushed.
- Redis stays healthy: **no network cuts or server restarts** in this suite.
- Concurrency 1 and prefetch multiplier 2. Late ACK by default; explicit early
  controls are labeled. Remote control, monitoring heartbeats, gossip, and mingle
  are disabled to avoid unrelated broker activity.
- Ordinary scan settings deliberately accelerated to **0.25 s interval / throttle
  1**. Default-cadence and throttled cases override these. These are experiment
  values, not recommended production settings.
- Most tasks retain results for 86400 s; the dedicated result-TTL case uses 2 s.
- A backend attempt counter makes repeat invocations short (0.12 s), bounding
  duplicate work without changing redelivery metadata or `request.retries`.
- Broker snapshots inspect queue lists, unacked hashes, receipt timestamps, ages,
  and key TTLs. Backend states are read uncached. Wrappers record actual original
  ACK returns, scan calls, restoration calls, receipt, acceptance, and body events.

**Interpretation:** body completion is not backend SUCCESS or broker settlement.
ACK counts below are successful `Message.ack()` returns, not counts of distinct
broker records removed. Restored copies retained their delivery tag. Repeated
ACKs can return successfully even after that record is already absent.

No explicit reject operations, ACK errors, Celery task-retry publications, or
incremented retry headers occurred in the final run. Fourteen SIGKILLs were
**planned crash injections**; no safety-deadline kill was needed.

## 5. B/R/T configuration matrix: only B controls delivery recovery

Each case starts an 8 s late-ACK task, SIGKILLs the solo worker after body start,
then launches a replacement with the same settings. Observe for 4 s after the
replacement is ready. `—` means the option is genuinely omitted, not set by an
inherited experiment module. Resolved channel/QoS values are recorded separately.

| Case | B | R | T | Effective timeout | Body starts / completes | ACK returns | Backend | Final broker state | Worker exits |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `config-none` | — | — | — | 3600 | 1 / 0 | 0 | PENDING | 1 unacked | -9, 0 |
| `config-T` | — | — | 2 | 3600 | 1 / 0 | 0 | PENDING | 1 unacked | -9, 0 |
| `config-R` | — | 2 | — | 3600 | 1 / 0 | 0 | PENDING | 1 unacked | -9, 0 |
| `config-RT` | — | 2 | 2 | 3600 | 1 / 0 | 0 | PENDING | 1 unacked | -9, 0 |
| `config-B` | 2 | — | — | **2** | **2 / 1** | 1 | SUCCESS | Absent after recovery/ACK | -9, 0 |
| `config-BT` | 2 | — | 2 | **2** | **2 / 1** | 1 | SUCCESS | Absent after recovery/ACK | -9, 0 |
| `config-BR` | 2 | 2 | — | **2** | **2 / 1** | 1 | SUCCESS | Absent after recovery/ACK | -9, 0 |
| `config-BRT` | 2 | 2 | 2 | **2** | **2 / 1** | 1 | SUCCESS | Absent after recovery/ACK | -9, 0 |
| `config-B12-R2-T2` | **12** | 2 | 2 | **12** | 1 / 0 | 0 | PENDING | 1 unacked | -9, 0 |
| `config-B2-R12-T12` | **2** | 12 | 12 | **2** | **2 / 1** | 1 | SUCCESS | Absent after recovery/ACK | -9, 0 |

The five short-timeout rows restored the original delivery at approximately
**2.21–2.25 s after registration**. Recovery did not increment the task retry count.
The default/12 s rows establish absence of recovery **within the observation
window**, not that the message can never recover.

All worker configurations also recorded:

- R preserved in backend options when supplied, but no visibility setting passed
  into the Redis backend client's connection parameters.
- T present only in explicitly configured cases; it did not change channel QoS.
- Channel and QoS visibility both equal B, or 3600 when B is absent.

The earlier connection probe already left R/T at 3600 while lowering B in its
short-visibility cases. These new cases remove that inherited configuration and
make the precedence/omission test explicit.

## 6. Recovery needs a scanning actor, not just an expired age

| Case | Setup | Observation | Body starts / completes | ACK returns | Final backend / broker | Exits |
| --- | --- | --- | --- | --- | --- | --- |
| `publisher-short-only` | Publisher B=2; original/replacement workers B=12 | No recovery in 4 s replacement window | 1 / 0 | 0 | PENDING / 1 unacked | -9, 0 |
| `no-consumer-after-crash` | Worker B=2; no replacement for 3.2 s | At age 3.24 s, still unacked, not queued; hash TTL **-1** | 2 / 1 after replacement starts | 1 | SUCCESS / absent | -9, 0 |
| `scan-defaults` | B=2; async scan interval/throttle left at installed defaults | No recovery in 4 s replacement window, despite age exceeding 2 s | 1 / 0 | 0 | PENDING / 1 unacked | -9, 0 |
| `scan-throttled` | B=2; interval 1 s, throttle 8 | Still unacked at 4 s; restored at age **8.73 s** when a scan runs | 2 / 1 | 1 | SUCCESS / absent | -9, 0 |

In the no-consumer case, the replacement's startup scan restored the delivery at
age **3.94 s**, shortly after that worker started. Redis had not expired or moved
it by itself. A publisher and the harness's inspection clients remained connected,
but neither ran a consumer recovery scan.

For this installed async transport:

- `unacked_restore_interval` defaults to **10 s**.
- `unacked_restore_throttle` defaults to **10**: only every tenth interval after
  the initial scan performs the recovery scan.
- Thus the approximate steady-state interval is **100 s**, plus scheduling and
  lock contention—not the visibility timeout itself.
- Startup performs an initial scan; an already-eligible delivery can therefore
  recover immediately on startup rather than waiting a full steady-state period.

The source establishes the default cadence; we observed only the short default
window, not a 100 s wait. These installed Kombu options/defaults must not be assumed
for every released version. The drain-events/gevent path has different scheduling.

## 7. Solo execution and mixed-worker scope

A 5 s body runs. When present, the second worker starts after the first body has
started and uses B=2. Both use the same backend. The first worker uses B=12 in
mixed-worker cases, deliberately disagreeing with the scanner.

| Case | ACK policy / workers | At about 3.3 s | Body starts / completes | ACK returns | Final backend / broker | Exits |
| --- | --- | --- | --- | --- | --- | --- |
| `solo-alone-late` | Late; one solo, B=2 | Original still unacked at age >3 s; no scan during body | 1 / 1 | 1 | SUCCESS / absent | 0 |
| `solo-alone-early` | Early; one solo, B=2 | **Still unacked**; early ACK requested but not yet executed | 1 / 1 | 1 | SUCCESS / absent | 0 |
| `solo-short-other-queue` | Late; owner B=12, second solo B=2 consuming **another queue** | Original copy restored to target queue while original body continues | **2 / 2, sequential** | 2 | SUCCESS / absent | 0, 0 |
| `solo-short-same-queue` | Late; owner B=12, second solo B=2 on same queue | Second body already completed while original still runs | **2 / 2, overlapping** | 2 | SUCCESS / absent | 0, 0 |
| `solo-early-short-same-queue` | **Early**; same two-solo setup | Same overlap despite early-ACK policy | **2 / 2, overlapping** | 2 | SUCCESS / absent | 0, 0 |
| `prefork-early-short-same-queue` | Early; prefork owner B=12, second solo B=2 | Original actually ACKed; nothing to restore | 1 / 1 | 1 | SUCCESS / absent | 0, 0 |
| `solo-isolated-prefix` | Late; solo owner B=12; second solo B=2 with **different broker prefix** | Original still unacked; second scanner cannot see it | 1 / 1 | 1 | SUCCESS / absent | 0, 0 |

### A lone solo worker can exceed the threshold without redelivery

Solo blocks its consumer loop during inline execution, including `time.sleep` in
these tasks. No restoration scan ran inside its body interval. With no other
scanner, the aged record remained intact until the task finished and its ACK ran.

That observation is **not a guarantee of duplicate-free solo execution**. Another
worker can scan the shared records while the first solo worker is blocked.

### Early ACK is not necessarily early on the Redis socket

Solo's acceptance callback marked the request acknowledged and queued the ACK,
but the loop could not run the actual Redis operation during inline work. The
second worker restored the delivery at about **2.22 s**, while the first task was
still running. Both bodies completed.

The prefork control actually settled the original delivery while the child ran,
so no visibility restoration occurred. This confirms why request-level ACK flags
must not be substituted for broker-state observations.

### “The shortest value wins” is not server-side negotiation

The guide also says the shortest setting among applications sharing a broker is
used. The mechanism is more specific:

1. Workers sharing the Redis database and broker key namespace see the same
   unacked hash/index.
2. Each scanner uses **its own** visibility timeout when inspecting that shared
   data. The record does not carry an owner's visibility duration.
3. A short-timeout worker can restore another worker's delivery, including one
   routed to a queue it does not consume.
4. The shortest configured value matters only if a consumer using it actually
   scans. A pure publisher does not install a broker-wide minimum; a blocked solo
   worker is not scanning during its body; a stopped application is not scanning.
5. A distinct broker `global_keyprefix` isolates the records in the measured case.
   Merely choosing different queue names does **not** isolate recovery policy.

Thus an ordinary default-3600 scanner sharing records with a 43200 worker can
restore eligible deliveries much earlier than 12 hours. That hour-scale example
is source-backed extrapolation from the measured 2-vs-12 s case, not an hour-long
experiment.

## 8. ETA expiry: one solo worker can accumulate and execute copies

Each case publishes one task with a 6 s countdown. There is no disconnect, worker
restart, or second worker. A waiting ETA does not occupy solo's execution loop,
so its transport can keep scanning and restoring messages.

| Case | B / ACK / dedup | Receipts | Body starts / completes | ACK returns | Backend / final broker | Exit |
| --- | --- | --- | --- | --- | --- | --- |
| `solo-eta-short-late` | 2 / late / False | **4** | **4 / 4** | 4 | SUCCESS / absent | 0 |
| `solo-eta-short-early-dedup` | 2 / **early** / True | **4** | **4 / 4** | 4 | SUCCESS / absent | 0 |
| `solo-eta-short-dedup` | 2 / late / **True** | **3** | **1 / 1** | 3 | SUCCESS / absent | 0 |
| `solo-eta-long` | **12** / late / False | 1 | 1 / 1 | 1 | SUCCESS / absent | 0 |

At the waiting snapshot, short-visibility cases had already received a restored
copy but had executed **zero** bodies. The same ETA was retained; redelivery did
not start a new 6 s countdown. Registration of each restored copy refreshed the
unacked receipt timestamp, while earlier local timer entries remained scheduled.

At the due time, the retained entries could each execute. Further expiry around
the due time, before deferred ACK callbacks drained, produced another receipt in
the non-deduplicated cases. **Exact copy counts depend on timer/ACK ordering**;
three vs four is not a universal formula. The assertions require the mechanism,
not an invariant number of duplicates.

The early-ACK row still holds waiting ETA messages unacked. In this suite both
application and task ACK policy are early, so the successful-task deduplication
gate is disabled despite its configuration switch being True.

The late-ACK dedup row received and ACKed three copies but executed the body once:
later traces saw successful completion. This does not establish safety against
concurrent copies on other workers before SUCCESS exists; the previous connection
report measured that failure mode even with dedup enabled.

**Why this differs from ETA reconnect:** reconnect cleanup cleared old timer
entries in the earlier tests. A visibility scan only restores Redis bookkeeping;
it does not remove an already-scheduled local request. Those are distinct recovery
paths and must not be extrapolated from each other.

## 9. Prefetched work can expire before its first execution

`prefork-reserved-expiry`: one prefork execution slot, prefetch multiplier 2,
B=2, late ACK. Publish a 5 s task, then a short task while the first runs.

At about 3.3 s:

- The first body is still running.
- The second message was received but its body has **never started**.
- Both original deliveries have been restored to the Redis queue by the live
  controller's scan, while their local requests remain present.

| Message | Receipts | Body starts / completes | ACK returns | Backend / final broker | Worker exit |
| --- | --- | --- | --- | --- | --- |
| Original long task | 2 | 2 / 2 | 2 | SUCCESS / absent | 0 |
| Initially reserved short task | 2 | 2 / 2 | 2 | SUCCESS / absent | Same worker, 0 |

The second task's short runtime did not protect it: **its pool-wait time already
exceeded visibility**. With one execution slot these copies execute sequentially;
restoration alone does not create extra execution capacity.

## 10. Result retention is a different clock

`result-expiry-independent` uses B=12, R=2, T=2, and **`result_expires=2`**.
A short solo task completes and ACKs normally.

| Observation | Body starts / completes | ACK returns | Backend | Result-key TTL | Broker |
| --- | --- | --- | --- | --- | --- |
| Just after ACK | 1 / 1 | 1 | SUCCESS | 2 s | Absent |
| After another 2.4 s | Still 1 / 1 | Still 1 | **PENDING** | -2: key absent | Still absent |

Worker exit is 0. Result expiration did not republish or redeliver the task.
PENDING here means no stored result, not proof that execution never happened.
Redis backend source uses `result_expires` to select SETEX/TTL; R does not control
that path. The configuration controls confirm the distinction: `config-BR`,
`config-BT`, and `config-BRT` set the corresponding visibility-looking values to
2 but retain SUCCESS with a measured result TTL of **86398 s** at the observation
snapshot, because their `result_expires` remains 86400.

Result retention should cover the period in which you need completed results for
recovery/deduplication, subject to application retention requirements.

## 11. Practical guidance

- Configure **B** on all consumers that share the broker's unacked namespace.
  Merely setting R/T or changing the publisher is not sufficient in this stack.
- Choose B against the **full expected unacked interval**: ETA delay, rate/pool
  waiting, execution until actual settlement, and an operational margin. Using
  only maximum task-body runtime can be insufficient.
- A longer value reduces premature recovery but delays abandoned-work recovery.
  No finite value both guarantees fast crash recovery and prevents duplicates for
  arbitrarily long unacked work.
- Solo's inability to scan during its body is not a reliability feature. Other
  workers can restore its messages; while ETA waits, even that solo worker scans.
- Early ACK changes recovery semantics, but solo's queued early ACK must not be
  assumed to have reached Redis before execution. Nor does it ACK a waiting ETA.
- Keep execution idempotent. Late-ACK successful-task deduplication can suppress
  repeated completed work, but it is not a distributed execution lock.
- Do not treat SUCCESS, PENDING, request ACK flags, or ACK return counts alone as
  the complete delivery state. Use separate execution/result/broker evidence.
- Use database-backed scheduling or another suitable long-term scheduling design
  rather than making Redis visibility extremely long solely for distant ETA work.
- Broker prefixes/database separation can isolate policies, but changing a live
  prefix is not an automatic migration: existing queued/unacked work stays in
  its old namespace. Backend key prefixes are separate from broker isolation.
- Connection-retry settings and `broker_channel_error_retry` do not alter this
  age threshold or repair every failed ACK. Visibility expiry here happened with
  healthy connections. See connection-report appendices A5–A6.

## Reproduce and inspect

```bash
mise run setup
mise exec -- uv run --locked --no-sync python redis-visibility/test-redis-visibility-probe.py
mise run redis-visibility

# Focused configuration and solo checks:
mise run redis-visibility \
  --case config-B --case config-RT --case config-B12-R2-T2 \
  --case solo-short-other-queue --case solo-early-short-same-queue \
  --case solo-eta-short-dedup
```

- Worker module: `redis-visibility/redis_visibility_probe.py`.
- Runner: `redis-visibility/verify-redis-visibility.py`.
- Observation tests: `redis-visibility/test-redis-visibility-probe.py`.
- Final [27-case summary](redis-visibility/redis-visibility-runs/20261114T190223.192003Z/summary.json).
- Each case contains per-worker configuration/log files, `events.jsonl`, and
  `result.json` with phase snapshots, execution events, assertions, and exits.
- Final console: `redis-visibility/redis-visibility-validation-final.log`.

Exploratory and earlier validation runs remain available. The first exploration
exposed an **instrumentation bug**, not a Redis recovery failure: scanned tags
arrive as bytes, and the observer initially tried to JSON-serialize them directly.
It now decodes only the logged representation and passes the original argument
unchanged to Kombu. Five wrapper tests cover byte/string tags, exception
propagation, and scan-context/throttle observation. Worker ERROR/CRITICAL log lines
also fail the final runner. No production Celery/Kombu/Redis code was changed.

## Appendix: interpretation and source references

### A1. Configuration wiring and initialization

`repos/celery/celery/app/base.py`, `Celery._connection`, passes
`conf.broker_transport_options` into the Kombu connection. The installed Redis
channel includes `visibility_timeout` in `from_transport_options` and has a
3600 s class default. `QoS.visibility_timeout` reads the channel value.

`repos/celery/celery/app/defaults.py` has no standalone visibility-timeout option. The checked
Redis result-backend code does not read a visibility timeout from backend options
or top-level configuration. It uses supported backend options selectively;
`repos/celery/celery/backends/base.py` handles result key prefixes and
`repos/celery/celery/backends/redis.py` handles result TTLs, retry policy, and Redis operations.

Channel settings and the QoS cached property are initialized for a connection;
changing `app.conf` is not a broadcast update to existing workers/channels.
Reconfigure/restart the relevant consumers rather than assuming a publisher's
configuration change updates their scan thresholds.

### A2. Record age, scan cadence, and shared scope

Installed Kombu `transport/redis.py`:

- `QoS.append`: HSET delivery data and ZADD receipt timestamp.
- `QoS.restore_visible`: throttle, compute cutoff, acquire shared mutex, inspect
  eligible scores, call `restore_by_tag`.
- `QoS.restore_by_tag` and `Channel._do_restore_message`: remove unacked entries,
  restore payload using its routing metadata, mark it redelivered.
- `QoS.ack` / `QoS.reject`: settle, discard, or explicitly restore independently
  of waiting for the age threshold.
- `MultiChannelPoller.maybe_restore_messages`: shared-index scan; not limited to
  tasks owned by the scanning worker or its currently consumed queue.
- `Transport.register_with_event_loop`: async periodic scan scheduling.
- `MultiChannelPoller.get`: separate drain-events recovery path without that timer.

There is no per-record Redis TTL for this visibility mechanism. The shared mutex,
configured scan limit, loop scheduling, and eligible backlog can further delay
restoration. This suite accelerates scans and avoids load/lock contention; it is
not a production recovery-latency benchmark.

### A3. What visibility does not guarantee

- **Cleanup can restore earlier.** Reachable shutdown/reconnect cleanup can restore
  an unacked message before its visibility age is reached; previous reports
  measured that path. A long B is not a guaranteed minimum redelivery delay.
- **Expiry does not cancel local work.** Existing bodies and timer entries can
  survive broker restoration, producing duplicates.
- **No record means no visibility recovery.** The prior lost-BRPOP-reply case left
  neither queued nor registered-unacked data. Increasing/decreasing B cannot
  restore a nonexistent record.
- **ACK failure can be ambiguous.** An ACK may have committed before its reply was
  lost. Reconnection is not necessarily ACK replay; see the connection appendix.
- **Task retry policy is separate.** These redeliveries retained retries=0. A
  `max_retries` budget does not cap broker visibility redelivery loops.
- **Result expiration is not message expiration.** `result_expires` controls stored
  results. Task `expires` and soft/hard execution limits are different policies,
  covered in the earlier reports rather than re-tested here.

Completed-task deduplication is in `repos/celery/celery/app/trace.py`. These cases use matching
application/task ACK policies; mixed per-task overrides are not covered. The
late-ACK gate, redelivery metadata, persistent backend, and available success
information matter. A retained local successful-request cache can also help, but
is not a durable guarantee across worker replacement.

### A4. Scope and maintenance

Results are specific to this checkout, installed transport sources, platform,
worker settings, and private healthy Redis server. No 3600 s/43200 s wait,
Redis restart/persistence test, Sentinel failover, clock skew, packet loss,
RabbitMQ, or eventlet experiment is implied.

Future runs should write new raw directories, compare outcomes, and update the
measured tables explicitly while preserving this explanation and appendix.

### A5. Deep source audit: actual wiring and other brokers/backends

A follow-up audit searched all **242 Python source files** in this Celery package
and the installed Kombu package, then traced the relevant constructors, option
lookups, backend selection, and consumer paths. This is broader than searching
for one exact assignment in the Redis transport.

#### Broker path

```text
Celery.connection_for_read / connection_for_write
  → Celery._connection
  → transport_options = dict(conf.broker_transport_options,
                             **explicit_connection_options)
  → Kombu.Connection.transport_options
  → transport/channel initialization
  → Redis Channel.visibility_timeout
  → Redis QoS.visibility_timeout
  → restore_visible cutoff
```

References: `repos/celery/celery/app/base.py:1290–1312`, installed Kombu
`connection.py:278–280`, `transport/virtual/base.py:494–499`, and Redis transport
`from_transport_options`, `QoS.visibility_timeout`, and `restore_visible`.

There is no merge of R or T into this path. Explicit **per-connection** transport
options can override B; B is the normal Celery configuration entry point, not
an assertion that direct Kombu callers cannot supply options themselves.

#### Backend path

`Celery._get_backend` selects a backend and constructs it with `app` and `url`.
The common backend constructor reads supported result settings; it does not
forward the entire application configuration into a broker transport.

RedisBackend builds redis-py connection parameters itself. Its supported backend
options are selectively read; no R visibility value is transferred into a Kombu
Redis channel or used as a backend delivery deadline. Its uses of Kombu utility
functions/error classes do not mean it creates the broker's Redis transport.

SentinelBackend inherits the Redis result operations and additionally reads
`master_name`, `min_other_sentinels`, and `sentinel_kwargs`. This is a genuine use
of `result_backend_transport_options`, but not of its `visibility_timeout` entry.

The RPC backend was checked separately because it **does** consume result
messages through Kombu. `ResultConsumer.start` and reconnect use
`self.app.connection()`, and result publishing uses the application's producer
pool. Thus its normal connection options come from **B**, not a separate R merge.
This is a connection-wiring finding, not a claim that every broker/RPC combination
has identical result-delivery semantics or is recommended.

#### Which built-in transports use the same-named broker option?

| Component | Source finding |
| --- | --- |
| Redis / Redis Sentinel broker | B feeds Redis channel/QoS visibility; Sentinel broker channel inherits the Redis mechanism |
| SQS broker | `transport/SQS/__init__.py` reads `self.transport_options['visibility_timeout']`; used for the `VisibilityTimeout` attribute when creating a queue |
| PGMQ broker | `transport/pgmq.py` reads the same transport option and passes `vt` into PGMQ read operations |
| SLMQ broker adapter | `transport/SLMQ.py` reads the transport option and passes it to queue creation |
| RabbitMQ/AMQP | No Redis-style visibility mechanism from this setting; broker consumer-ack timeouts are separate |
| Redis / Sentinel result backend | No direct visibility effect from R or T; other backend options are used |
| RPC result backend | Uses application broker connections; does not merge R visibility into them |
| Other built-in result backends | No visibility-timeout consumer of R or T found; some key/value backends use other R entries through their common base |

These other-broker findings are **source-backed**, not live SQS, PGMQ, SLMQ,
Sentinel, or RabbitMQ experiments. In particular, SQS is not Redis emulation:
`_new_queue` returns an existing/predefined queue URL without updating its timeout.
Setting B is therefore not a promise to change an already-existing SQS queue.
There is no built-in SQS result-backend alias in this checkout; the configuration
reference's R visibility example mentioning SQS does not establish one.

`result_backend_transport_options` is **not generally ineffective**. Supported
entries include Redis retry policy, chord ordering, additional connection errors,
Sentinel options, and `global_keyprefix` in the common key/value backend. Nor are
other `result_backend_*` settings being called ineffective: retry, thread-safety,
and other result-backend settings have real consumers.

No independent top-level T reader was found in the checked built-in Celery
runtime paths. Custom loaders, application code, and third-party backend plugins
can read arbitrary configuration keys; they are outside that conclusion.

### A6. Git blame and why the documentation says to set all three

The local repository contains full history. Blame/show and the public GitHub
issue/PR discussions establish this sequence:

| Date | Change | Evidence and significance |
| --- | --- | --- |
| **2017-10-08** | [`7d52b0bddf`, PR #4144](https://github.com/celery/celery/commit/7d52b0bddfcac369acc750c532c2bb08012721a5) | Added Redis Sentinel result backend and the R options container. Its configuration docs already used `{'visibility_timeout': 18000}` as an example, although the implementation read Sentinel-specific options rather than visibility. |
| **2022-12-13** | [Comment in #7651](https://github.com/celery/celery/issues/7651#issuecomment-1349947072) | A user reported that setting all three values fixed their problem. The comment supplied a workaround assertion, not an isolated B-vs-R-vs-T comparison or a corresponding core code change. |
| **2022-12-14** | [Maintainer reply](https://github.com/celery/celery/issues/7651#issuecomment-1350803840) | Asked for a documentation PR. |
| **2023-08-22** | [`8ae0b22959`, PR #8451](https://github.com/celery/celery/commit/8ae0b229596cc8aeea4fb71020d9358a59338e08) | Added the R and T assignments to the Redis guide. The PR description quotes the 2022 workaround. **Documentation-only change**; no runtime support was added. |
| **2024-09-13** | [`235368ceb2`, PR #9264](https://github.com/celery/celery/commit/235368ceb2416fb0484b41d14484622a1c39fc24) | Changed the wording to **“required to set all of them”** and added the shortest-value warning, citing issues #7651 and #5935. Again, documentation only. |
| **2024-09-15** | [`ddf2ae124`, PR #9266](https://github.com/celery/celery/commit/ddf2ae124ae0b19ee0255ae3f84339a3d72b0d98) | Added matching values to smoke-test configuration “To match #9264's fix.” This changed fixtures, not runtime wiring, and did not independently test whether R/T were necessary. |

The direct reason for the three-line guide snippet is therefore traceable:
**a reported workaround was copied into documentation and later described as
mandatory**. That is not evidence of three independently implemented visibility
settings. The older R configuration example was introduced with Sentinel backend
support, whose actual option readers were Sentinel-specific.

I also checked Celery runtime source at both guide-change commits: the broker
connection builder still forwarded B rather than R/T. No literal
`visibility_timeout` reference was found in their `celery/` runtime trees.
This supports the wiring conclusion at those revisions without claiming to have
re-run their historical dependency stacks.

#### The linked issue contains a separate, credible interference explanation

[Issue #5935, April 2020 comment](https://github.com/celery/celery/issues/5935#issuecomment-621855974)
reported a metrics scraper creating a Celery instance and starting a transport
event loop with default configuration. Its `restore_visible` calls restored tasks
earlier than the task workers' longer setting. Later comments reported similar
interference between projects sharing Redis.

This is consistent with the source and our different-queue scanner experiment.
It means the audit must include **all processes actually running recovery scans**,
not just task workers: monitoring/inspection tools can matter if they start such
consumers. Merely constructing a Celery object, publishing tasks, or reading Redis
keys is not equivalent to running a recovery scan.

The thread also contains reports involving SQS, differing scan delays, solo, and
changes to deployments. It is not one controlled reproduction of one root cause.
We cannot reconstruct why adding all three assignments appeared to fix the 2022
reporter's setup. The historical interference report does not prove that it was
also that reporter's exact cause, and its proposed infinity workaround is not a
configuration recommendation here.

### A7. Important qualification: indirect configuration effects are possible

“No independent core visibility effect from R/T” must not be shortened to
“assigning anything through those names can never affect visibility.”

#### Shared dictionaries

The original workaround used chained assignment:

```python
result_backend_transport_options = broker_transport_options = {
    'visibility_timeout': 600,
}
```

That creates **one mutable dictionary with two references**. For example:

```python
shared = {'visibility_timeout': 600}
app.conf.broker_transport_options = shared
app.conf.result_backend_transport_options = shared
app.conf.result_backend_transport_options['visibility_timeout'] = 1200
# B is now 1200 too, because both options reference shared.
```

Here a mutation through R changes B, which the broker reads. It does **not** mean
RedisBackend implements an R visibility timer. Reassigning R to a separate
dictionary does not have this aliasing effect. Our real-worker matrix uses
independent dictionaries; its conclusion remains valid.

#### Explicit application/fixture forwarding

Application code can use T as its own configuration variable:

```python
app.conf.visibility_timeout = 600
app.conf.broker_transport_options = {
    'visibility_timeout': app.conf.visibility_timeout,
}
```

That works because the application **copies the value into B**, not because Celery
recognizes T as an independent transport setting. Changing T later alone does not
update the already-built dictionary/channel. Current smoke fixtures explicitly
copy T into the transport dictionaries, which must not be mistaken for implicit
core support.

#### Additional verification and retained evidence

[`redis-visibility/test-visibility-config-wiring.py`](redis-visibility/test-visibility-config-wiring.py) adds
**nine passing source-path tests**, separate from the 27 real-worker cases:

- All eight presence combinations, using distinct B/R/T values, and explicit
  per-connection overrides.
- Generic Celery connection construction for five broker schemes with the final
  constructor mocked; no optional broker drivers/services are exercised.
- Tracked Redis configuration reads showing supported backend options are used
  while R visibility and top-level T are not read on the exercised paths.
- Sentinel backend option forwarding with Sentinel operations mocked.
- A second key/value backend honoring result-key prefixes.
- RPC consumer connection wiring using an in-memory broker.
- Aliased dictionary mutation and explicit application forwarding.

```bash
mise exec -- uv run --locked --no-sync python redis-visibility/test-visibility-config-wiring.py
```

Audit artifacts under `redis-visibility/redis-visibility-audit/` include
[git evidence](redis-visibility/redis-visibility-audit/git-evidence.txt),
[source hashes and search inventory](redis-visibility/redis-visibility-audit/manifest.json),
[test output](redis-visibility/redis-visibility-audit/config-wiring-tests.log), and public GitHub
issue/PR JSON snapshots. No production files, broker services, or historical
checkout environments were changed by this audit.
