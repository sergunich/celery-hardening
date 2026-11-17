# Redis worker connections, heartbeats, reconnects, and delivery recovery

> Redis-only follow-up to [shutdown](shutdown.md),
> [ETA/prefetch](eta-prefetch.md), and [task policies](task-policy.md).
> **47/47 selected runtime cases passed**, including reproduced limitations and
> a delivery-loss window. PASS means the asserted behavior was observed, not that
> every failure preserves work. No RabbitMQ experiments or Docker startup occurred.
>
> This report and its appendix are maintained text. The scripts write timestamped
> raw-run directories, **not this report**, so future runs preserve explanations.

## Direct answers

### `--without-heartbeat` is not “disable connection failure detection”

There are several different mechanisms:

| Mechanism | Purpose | Redis behavior / setting |
| --- | --- | --- |
| Celery **worker-monitoring heartbeat** | Announces worker liveness/activity to monitoring tools and other event consumers | `--without-heartbeat` disables this service; it does not disable Redis reconnection or Redis health checks |
| **Broker-protocol heartbeat** | Detects a dead connection using the broker protocol, notably AMQP | Redis transport reports **`supports_heartbeats=False`** and negotiated interval **0**, even with `broker_heartbeat=2` configured |
| **Redis client/transport health checks** | PING/check response progress; detect stale Pub/Sub and BRPOP connections | `broker_transport_options['health_check_interval']`; implementation depends on Kombu/redis-py version |
| **Socket operation timeout** | Bounds an individual blocking socket operation | `socket_timeout`; not a universal deadline for an asynchronous BRPOP wait |
| **TCP keepalive** | OS-level probing of an otherwise idle TCP peer | `socket_keepalive` and OS-specific options; not tested here |

Disabling monitoring events can reduce traffic **if you do not need that
monitoring**. It is not a general recommendation to disable connection health
checks. Our earlier workers used `--without-heartbeat` to reduce experiment
noise, not as production configuration advice.

Redis does not disconnect a worker because it missed a Celery `worker-heartbeat`
event. The private server's Redis idle-client timeout was 0 (disabled). A configured
Redis idle timeout and TCP keepalive are separate mechanisms, not Celery heartbeat
deadlines.

### What does the warm-shutdown documentation mean?

The [Workers Guide](https://docs.celeryqen/stable/userguide/workers.html#warm-shutdown)
says that before the prefork warm-shutdown heartbeat fix:

> This caused the broker to terminate the connection, which meant that tasks were not able to complete.

The local `repos/celery/docs/userguide/workers.rst` contains this passage in its **5.6 change
note about maintaining broker heartbeats during prefork warm shutdown**. Those
protocol heartbeats are not the service disabled by `--without-heartbeat`, and
Redis does not use them.

“Tasks were not able to complete” is too broad to interpret as “every Python task
body immediately stopped when a connection closed.” Distinguish:

1. Body execution and side effects.
2. Result-backend storage.
3. Delivery settlement at the broker.
4. Whether the worker can consume more work or finish shutdown cleanly.

Our Redis outage cases completed task bodies and stored **SUCCESS**, but sometimes
left the original delivery **unacked**. A supported cancellation policy can stop
an active task instead; application code that uses the failed broker can also
raise its own errors. We did **not** reproduce the historical pre-5.6 bug or test
an AMQP broker, so these measurements do not establish its exact historical
failure mode.

### “It will just reconnect” omits two problems

- **Detection must happen first.** An explicit socket close is different from a
  silent connection that never produces another response. Retry settings cannot
  help while an operation is blocked and has not raised an error.
- **Reconnection is not delivery recovery or task retry.** It can restore new
  consumption while an old successful task remains unacked. It can also cause
  redelivery, duplicate work, or leave a message lost in a pop/registration gap.

An important Redis-specific correction to a generic AMQP explanation: **a socket
reconnect does not always make the old Redis ACK impossible**. Redis ACK emulation
removes entries from Redis data structures using the delivery tag; it can do so
through a reconnected Redis client. We measured that behavior. Do not transplant
AMQP's channel-local delivery-tag semantics unchanged onto Redis.

## Reduced matrix and rationale

| Area | Cases | Why explicitly test it? |
| --- | ---: | --- |
| Monitoring during execution | 6 | All four pools, solo disabled-monitor control, and non-yielding gevent work |
| Idle connection cuts | 4 | All pools; three consecutive cuts for solo/prefork check continued recovery |
| Cut during active work | 10 | All pools × cancellation off/on, plus early-ACK solo/prefork controls |
| Sustained outage during active work | 2 | Solo vs prefork ACK timing differs; backend stays reachable |
| Startup/runtime retry switches and retry cap | 4 | Shared connection policy, isolated in solo rather than multiplying across pools |
| Silent BRPOP socket | 4 | Solo/prefork async loops vs gevent drain loop, plus health-check-disabled control |
| Silent monitoring publisher socket | 2 | Show why a blocked synchronous operation needs a socket timeout |
| ACK transaction boundary | 3 | Before commit, after commit with reply lost, and completed-task deduplication |
| Popped delivery reply lost | 1 | Test whether “not ACKed” always implies recoverable unacked state |
| Visibility recovery and overlap | 4 | Actual expiry after crash, default-timeout control, and concurrent duplicates including solo |
| ETA across reconnect | 2 | Solo hub timer vs gevent timer implementation; prevent old/new timer duplication |
| Warm shutdown with/without outage | 3 | Address the documentation question without claiming AMQP coverage |
| Prefetch-reduction switch | 2 | One active task, concurrency 2, enough initial credit to expose a reduction if applied |
| **Total** | **47** | |

### What we reuse or extrapolate

- Prior reports establish solo's inline blocking, delayed early ACK, ordinary
  failure ACK policy, and unsupported solo/thread cancellation. We did not repeat
  every task-failure flag or shutdown signal permutation.
- Connection-loss cancellation was **not** inferred from QUIT: gevent's previously
  measured signal-context error does not establish behavior in the consumer's
  ordinary error handler. Both paths were therefore tested separately.
- Startup/runtime retry selection is shared consumer code. The policy decisions
  can be extrapolated to other pools; exact shutdown timing after giving up cannot.
- The thread pool uses the async consumer loop in this setup, like solo/prefork.
  Its silent-BRPOP detection is source-backed rather than another measured row.
  Its ordinary reconnect and monitoring behavior were measured directly.
- Socket-level Redis ACK and pop/registration mechanisms are shared across pools.
  The boundary faults are injected in solo to isolate them; timing under other
  pools can differ, particularly when another execution is still running.
- Waiting ETA cleanup is shared; solo and gevent cover distinct timer models.
  Prefork/thread ETA-reconnect combinations are not separately measured here.
- Redis server restart, persistence loss, Sentinel failover, replica promotion,
  real packet-level network partitions, TLS, RabbitMQ, and eventlet are **not**
  covered by these socket/proxy tests.

## Environment and fault model

Same Celery **5.7.0** checkout, revision
`6fcdbf904d410749ad7676d8ff6b1b25d6e19675`; CPython **3.13.9**, macOS **15.8.1 ARM64**,
Kombu **5.7.0**, redis-py **8.1.0**, Billiard **4.3.2**, gevent **26.9.0**,
greenlet **3.5.6**, Redis server **8.2.3**. Prefork uses **spawn**, not Linux fork.
Source/script hashes are saved in the final summary.

```text
Worker broker connections → loopback TCP fault proxy → private Redis Unix socket, DB 0
Worker result backend    ───────────────────────────→ same Redis, DB 1 (proxy bypass)
Harness publisher/admin/monitor ────────────────────→ Redis directly
```

**The Redis server remains running.** Only worker-to-broker connections through
the proxy are faulted. Although the backend uses the same private server, its
network path is deliberately independent. A production Redis outage affecting
both broker and backend is a different experiment.

The proxy is restricted to localhost and owned sockets; no firewall or system
network settings change. The Redis server has persistence disabled for isolation,
not because that is production advice. Server `timeout=0`, `tcp-keepalive=300`;
client-side TCP keepalive is disabled in these tests.

### Accelerated test settings—not production recommendations

| Parameter | Tests | Installed transport's normal default / qualification |
| --- | --- | --- |
| Concurrency | 1, except explicit concurrency-2 rows | Independent of connection health |
| Prefetch multiplier | 2 | Leaves credit for observing the active-task connection path |
| Late ACK | True unless early-ACK control | Celery normally defaults to False |
| Monitoring heartbeat interval | 1 s when enabled | Celery normally uses 2 s |
| `health_check_interval` | 1 s, or 0 in disabled-health control | 25 s in this Kombu version |
| `brpop_timeout` | 1 s | Server-side blocking-pop timeout, not a task deadline |
| `socket_timeout` | 1 s, except explicit unbounded-publish case (`None`) | Normally `None` |
| `socket_connect_timeout` | 0.5 s | Bounds TCP establishment, not every later operation |
| Visibility timeout | 3600 s, except explicit 2 s cases | Short values deliberately demonstrate recovery/duplicates |
| Unacked scan interval / throttle | 0.5 s / 1 | Normally 10 s / 10 in the installed async path |
| Connection retries | Startup/runtime enabled, max 4 unless overridden | Test cap bounds cleanup; not a universal recommendation |
| Remote control | Enabled except silent-BRPOP/delivery-gap cases | `--without-gossip` and `--without-mingle` do not disable it |

All workers use the real Celery CLI. Gevent receives its normal early monkey patch.
Tasks use a healthy result backend and do not publish application tasks. A
backend-side attempt counter makes a redelivery's body short (0.15 s), bounding
duplicate execution without modifying `retries` or redelivery flags.

Faults:

- **Cut:** close existing proxied sockets; new connections can succeed immediately.
- **Outage:** close existing sockets and reject new ones for a specified window.
- **Silent socket:** keep TCP connections open but drop application bytes on an
  already-established BRPOP or publisher connection. New connections remain
  healthy. This models a stalled path; it does not measure OS TCP retransmission
  or keepalive behavior.
- **ACK boundary:** stop the actual Redis ACK transaction before EXEC, or discard
  its reply only after receiving Redis's EXEC response. Direct Redis inspection
  independently confirms whether the unacked record survived.

### Reading the results

- **Started / completed** count task-body executions, not message publications.
- **ACK returns** count successful returns from the original `Message.ack()`.
  They do **not** establish that every ACK was uniquely applied, or that an ACK
  whose reply was lost did not commit. Redis state is inspected separately.
- **Unacked** means a record actually exists in Redis's unacked hash.
- **Redelivery** is observed through receipt metadata and actual execution, not
  inferred solely from an empty queue or `RETRY` backend state.
- **Automatic vs rescue:** silent-socket phases are sampled **before** the harness
  closes the stale socket. Post-rescue success is never credited to automatic recovery.
- A new ping task verifies subsequent consumption in recovery cases. Worker exit,
  planned SIGKILL, and any safety-deadline kill are separate fields.

No tested work message used an explicit reject operation or published a Celery
retry. All observed body retries retained `request.retries=0`.

## 1. Monitoring heartbeats while tasks execute

Cases use a 2.8 s task and configured `broker_heartbeat=2`. All reported negotiated
Redis heartbeat **0** and `supports_heartbeats=False`.

| Case | Monitoring enabled? | Body behavior | Actual monitoring heartbeats during body | Body / backend / ACK | Worker exit |
| --- | --- | --- | --- | --- | --- |
| `monitor-solo` | Yes | Inline sleeps | **None** | Completed / SUCCESS / ACK returned | 0 |
| `monitor-prefork` | Yes | Sleeps in child | Continue | Completed / SUCCESS / ACK returned | 0 |
| `monitor-threads` | Yes | Sleeps in thread | Continue | Completed / SUCCESS / ACK returned | 0 |
| `monitor-gevent` | Yes | Cooperative sleeps | Continue | Completed / SUCCESS / ACK returned | 0 |
| `monitor-solo-disabled` | No | Inline sleeps | None before, during, or after | Completed / SUCCESS / ACK returned | 0 |
| `monitor-gevent-busy` | Yes | Non-yielding CPU loop | **None** | Completed / SUCCESS / ACK returned | 0 |

Heartbeats were observed through a separate Redis Pub/Sub subscriber, not merely
Celery's pre-send `heartbeat_sent` signal. Solo and busy gevent sent monitoring
heartbeats outside the body interval but not during it. Redis did not terminate
them for missing these events.

**Solo's limitation is event-loop progress, not inability to run the body.**
Sleeping or blocking I/O inside a solo task does not yield to Celery's event loop.
Monitoring, connection-loss handling, and periodic transport checks can therefore
wait until the task returns. A custom task that itself performs broker operations
can encounter an error sooner; this suite deliberately isolates broker I/O from
business work.

## 2. Explicit socket closes and retry policy

### Idle reconnect

| Case | Consecutive cuts | Consumer generations observed | Fresh work after each cut | Result / ACK | Worker exit |
| --- | --- | --- | --- | --- | --- |
| `idle-reconnect-solo` | 3 | 4 | Executed | Each SUCCESS and ACKed | 0 |
| `idle-reconnect-prefork` | 3 | 4 | Executed | Each SUCCESS and ACKed | 0 |
| `idle-reconnect-threads` | 1 | 2 | Executed | SUCCESS and ACKed | 0 |
| `idle-reconnect-gevent` | 1 | 2 | Executed | SUCCESS and ACKed | 0 |

Monitoring heartbeats were disabled in all four cases. They are not required for
recovery from an explicit socket close. Repeated cuts confirm continued operation
in these runs; they are not a long-duration reconnect/leak stress test.

### Startup and runtime are separate decisions

| Case | Configuration / fault | Outcome | Task result / ACK | Worker exit |
| --- | --- | --- | --- | --- |
| `startup-retry-solo` | Startup retry True, runtime retry **False**; broker path initially down for 1 s | Retries initial connection and becomes ready | Fresh ping SUCCESS / ACK | 0 |
| `startup-no-retry-solo` | Startup retry **False**, runtime retry True; path initially down | Gives up before ready | No task executed | 1 |
| `startup-max-retries-solo` | Startup retry True, max retries 1; path remains down | Exhausts retry budget before ready | No task executed | 1 |
| `runtime-no-retry-solo` | Starts connected; runtime retry **False**; then socket cut | Exits instead of restarting the consumer | No work task in this case | 1 |

`broker_connection_retry_on_startup` controls initial establishment.
`broker_connection_retry` controls recovery after an established connection is
lost. `broker_connection_max_retries` bounds connection retry attempts, **not task
executions**. In this checkout, an unset startup setting falls back to the older
runtime setting; that fallback is source-checked rather than another runtime row.

## 3. Cut during an active task: timing and cancellation matter

A 4 s task starts, then existing sockets are cut at about 0.3 s. The broker path
is immediately available for new connections. All cases below use late ACK and
concurrency 1 unless stated otherwise.

| Case | Cancel-on-loss | When consumer handles loss | Body starts / completes | ACK returns | Redelivery | Final backend / broker | Exit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `active-solo-keep` | False | **After body returns**, about 4 s | 1 / 1 | 1 | None | SUCCESS / absent | 0 |
| `active-solo-cancel` | True | **After body returns**, no active request left | 1 / 1 | 1 | None | SUCCESS / absent | 0 |
| `active-prefork-keep` | False | While body runs, about 0.3 s | 2 / 2 | 2 | Yes; duplicate body execution | SUCCESS / absent | 0 |
| `active-prefork-cancel` | True | While body runs | 2 / 1 | 1 | Yes; first body terminated, redelivery completes | SUCCESS / absent | 0 |
| `active-threads-keep` | False | While body runs | 2 / 2 | 2 | Yes; duplicate body execution | SUCCESS / absent | 0 |
| `active-threads-cancel` | True | While body runs; cancellation raises `NotImplementedError` | 2 / 2 | 2 | Yes; original continues | SUCCESS / absent | 0 |
| `active-gevent-keep` | False | While body runs | 2 / 2 | 2 | Yes; duplicate body execution | SUCCESS / absent | 0 |
| `active-gevent-cancel` | True | While body runs; greenlet cancellation succeeds | 2 / 1 | **2** | Yes; redelivery completes | SUCCESS / absent | 0 |

The immediately reachable Redis path lets old-connection cleanup restore the
unacked delivery. In non-solo cases the original task can still be executing when
the restored message is fetched again. With one execution slot, another body
may wait for the first; the concurrency-2 cases below also demonstrate overlap.

**Why solo differs:** the queued ACK runs after the body returns and can reacquire
a working Redis connection. In the measured cut case it removed the unacked record
before the consumer processed its stale BRPOP connection. By then there was no
active task for cancellation. This is a measured ordering, not a promise that solo
avoids redelivery under all faults.

**Why gevent differs from QUIT:** the connection-error handler is not the OS-signal
handler context that previously raised `BlockingSwitchOutError`. Cancellation
works here. Its canceled first request still takes the pool success-callback ACK
path, but the original message had already been restored, so this run recovered
work through redelivery. An ACK return is not proof that its body finished.

**Threads:** the connection-loss handler catches the unsupported cancellation
error and continues reconnecting. Enabling cancellation does not create a thread
kill implementation.

The option `worker_cancel_long_running_tasks_on_connection_loss` selects active,
unacknowledged late-ACK tasks; it does not check a separate “long-running” duration
threshold. Its cancellation path can mark a backend result RETRY, but no new task
message was published and retry headers did not increment in these cases.

### Early-ACK controls

| Case | Cancel-on-loss | Original execution | ACK timing / disposition | Recovery | Exit |
| --- | --- | --- | --- | --- | --- |
| `active-solo-early` | True | One body completes, SUCCESS | Early ACK requested at acceptance but transport ACK completes after body return | No duplicate in this cut window | 0 |
| `active-prefork-early` | True | One body completes, SUCCESS | Original already ACKed before cut | Not canceled or redelivered | 0 |

Early-ACK tasks are excluded by the cancellation condition. For solo, the request's
local acknowledged flag and the actual Redis operation can still be separated in
time, as the earlier reports established.

### Sustained outage: successful task does not imply successful ACK

A 2 s task runs; the path is unavailable from about 0.3 s until about 4.3 s. Monitoring
is disabled and cancellation is off. The result backend stays healthy.

| Case | Body / backend during outage | ACK returns / errors | After reconnection | Old delivery after worker exit | Exit |
| --- | --- | --- | --- | --- | --- |
| `outage-solo` | Completed / SUCCESS | **0 / 1** | Fresh ping executes successfully | **Still unacked**, not queued; default visibility timeout not waited out | 0 |
| `outage-prefork` | Completed in child / SUCCESS | **1 / 0** | Parent processes result/ACK after reconnect; fresh ping executes | Absent | 0 |

The prefork result callback was processed after the connection-retry wait and path
recovery, allowing Redis settlement then. Solo attempted its queued ACK while the
path was still unavailable; it failed. Reconnection did not repair that failed
ACK in the tested window. These timings explain the outcomes; changing outage
length or task duration can change them.

## 4. Silent connections: retry needs a detectable failure

### BRPOP response never arrives

An established BRPOP socket is blackholed while new connections remain usable.
The old server-side BRPOP is allowed to time out before publishing a ping task,
so these rows isolate stale-socket detection rather than losing that task's reply.
Sample after a further 4 s, **before manual rescue**.

| Case | Health interval | Socket timeout | Before rescue | Automatic recovery mechanism | Final result / ACK / exit |
| --- | --- | --- | --- | --- | --- |
| `halfopen-brpop-solo` | 1 s | 1 s | Ping already completed | Transport drops overdue BRPOP socket and reconnects | SUCCESS / ACK / 0 |
| `halfopen-brpop-prefork` | 1 s | 1 s | Ping already completed | Same async transport check | SUCCESS / ACK / 0 |
| `halfopen-brpop-gevent` | 1 s | 1 s | Ping already completed | BRPOP check in `drain_events` polling path | SUCCESS / ACK / 0 |
| `halfopen-brpop-no-health` | **0** | **1 s** | Ping still queued; body never started | **No recovery in observation window**; harness closes stale socket afterward | SUCCESS / ACK / 0 only after rescue |

In the first three cases, the full Celery consumer did not restart: the Redis
transport repaired its BRPOP socket underneath it. “Connected” log counts alone
would miss this recovery.

A socket timeout does not bound an async wait that never calls a blocking read:
when no bytes arrive, the poller never invokes that read. The BRPOP health check
compares the outstanding command's age against its server-side timeout plus the
health interval. In this installed Kombu 5.7, the documented approximate bound is
`brpop_timeout + 2 * health_check_interval` **when the loop progresses**. Older
Kombu versions must not be assumed to have this BRPOP watchdog.

### Synchronous monitoring publication never returns

Only the established monitoring publisher's socket is blackholed. Task-consuming
sockets and new connections remain usable. A queued ping task checks whether the
controller continues dispatching work.

| Case | Monitoring | Health interval | Socket timeout | Before rescue | Afterward |
| --- | --- | --- | --- | --- | --- |
| `halfopen-monitor-unbounded` | Enabled | 1 s | **None** | Solo controller blocked publishing a heartbeat; ping still queued | Manual stale-socket close releases it; ping SUCCESS and ACK; exit 0 |
| `halfopen-monitor-bounded` | Enabled | 1 s | **1 s** | Publisher operation times out/reconnects; ping already SUCCESS and ACK | No rescue needed for task progress; exit 0 |

Redis health checks are not an independent watchdog thread that can rescue any
blocking operation. A synchronous publish can prevent the event loop from running
those checks. A socket timeout bounds that operation; application/Kombu retry
loops may still add further time, so it is not a total wall-clock deadline for
every surrounding operation.

This does not mean monitoring should always be disabled. It means monitoring I/O
needs a failure policy too. The optional monitoring service and connection-health
mechanisms solve different problems.

## 5. ACK ambiguity: distinguish Redis commit from the client's observation

Solo completes a 1 s task and stores SUCCESS. The proxy faults the actual ACK
transaction. The path remains down for 2.5 s, then reconnects. Visibility timeout
is 2 s, with accelerated recovery scans.

| Case | Wire fault / deduplication | Redis state immediately after fault | Deliveries received / bodies executed | ACK returns / ACK errors | Final result / broker | Exit |
| --- | --- | --- | --- | --- | --- | --- |
| `ack-before-commit-repeat` | Stop before EXEC; dedup False | SUCCESS; original still unacked | **2 / 2** | 1 / 1 | SUCCESS / absent after redelivery ACK | 0 |
| `ack-before-commit-dedup` | Stop before EXEC; dedup True | SUCCESS; original still unacked | **2 / 1** | 1 / 1 | SUCCESS / absent after duplicate is skipped and ACKed | 0 |
| `ack-reply-lost` | Redis commits ACK; proxy discards EXEC reply | SUCCESS; **unacked record already gone** | **1 / 1** | **0 / 1** | SUCCESS / absent; nothing redelivered | 0 |

**An ACK error does not establish whether Redis applied it.** The last row has
no successful return from `Message.ack()`, yet the delivery is already settled.
The first two rows have a superficially similar client error but a recoverable
unacked record instead. Direct Redis inspection distinguishes them in the test;
an application ordinarily cannot assume which side of the boundary failed.

`worker_deduplicate_successful_tasks=True` skipped the body only after the restored
message arrived with its redelivery flag and Redis backend SUCCESS was available.
It did not prevent delivery, make ACK/result storage atomic, or undo side effects.
It is not the same as task retries or an exactly-once execution guarantee.

## 6. Delivery reply lost before unacked registration

**Case `delivery-reply-lost`:** the worker has issued BRPOP. Publish a task while
that pop is outstanding, then discard the server's actual message-bearing reply.
The proxy records the original task ID in that dropped reply. Restore the socket,
wait beyond the configured 2 s visibility timeout, and verify another ping runs.

| Task body | Backend | ACK / reject | Redis queue | Redis unacked hash | Recovery after reconnect/expiry | Exit |
| --- | --- | --- | --- | --- | --- | --- |
| Never received or started | PENDING | None | **Absent** | **Absent** | **None**; fresh ping succeeds | 0 |

The Redis transport's sequence is not an atomic broker-native delivery with
server-owned unacked state:

```text
Redis BRPOP removes message from list
  → reply crosses connection
  → Kombu receives/decodes it
  → Kombu records delivery in Redis unacked structures
  → task dispatch
```

Losing the reply between pop and registration leaves no queued message and no
unacked record for a visibility scan to restore. This is a narrowly targeted,
reproduced loss window—not a claim that ordinary disconnects always lose tasks.
It also shows why `acks_late=True` plus `visibility_timeout` is not an unconditional
at-least-once guarantee across every Redis transport failure window.

The silent-BRPOP health tests deliberately avoided this window so the lost task
would not be mistaken for a failed reconnect.

## 7. Visibility timeout: recovery and overlapping execution

The old experiments inspected default-timeout unacked records without waiting
3600 s. These cases actually exercise expiration using 2 s and faster scans.

| Case | Setup | Body starts / completes | Backend | ACK returns | Recovery / remaining state | Worker exits |
| --- | --- | --- | --- | --- | --- | --- |
| `visibility-crash-short` | Solo task starts; SIGKILL worker; launch replacement; visibility 2 s | 2 / 1 | SUCCESS | 1 | Original delivery redelivered and completed | -9, then 0 |
| `visibility-crash-default` | Same, visibility **3600 s**; observe replacement for 4 s | 1 / 0 | PENDING | 0 | Original still unacked; fresh replacement ping succeeds | -9, then 0 |
| `visibility-overlap-prefork` | Healthy connections, concurrency 2, first body 5 s, visibility 2 s, dedup True | **2 / 2, overlapping** | SUCCESS | 2 | Short timeout redelivers while original still executes | 0 |
| `visibility-solo-other-worker` | First solo runs 5 s; launch another solo; visibility 2 s, dedup True | **2 / 2, overlapping** | SUCCESS | 2 | Other worker scans/restores and executes original delivery | 0, 0 |

There is **no connection fault** in the overlap cases. A too-short visibility
timeout alone creates duplicates. Solo's blocked local scan is no protection:
another consumer can restore its delivery while it still runs.

Completed-task deduplication does not prevent concurrent execution when the
backend is not yet SUCCESS at the second trace's entry. Both overlap cases enable
it and still execute two bodies. The fixture makes the second invocation short;
it demonstrates overlapping invocations, not a second full 5 s workload.

Recovery is driven by the transport's scans, not an exact Redis-native task timer.
Shortening the timeout also increases premature redelivery risk. Increasing it
reduces that risk but delays recovery of genuinely abandoned work. ETA waiting,
pool waiting, and runtime can all contribute to the unacked interval.

## 8. ETA timers and prefetch across reconnect

### Waiting ETA

Publish one task due in 4 s, wait until it is scheduled but not executing, then
cut sockets while new connections remain healthy.

| Case | Received | Body starts / completes | ACK returns | Backend / broker | Exit |
| --- | --- | --- | --- | --- | --- |
| `eta-reconnect-solo` | Twice: original + restored | **1 / 1** | 1 | SUCCESS / absent | 0 |
| `eta-reconnect-gevent` | Twice: original + restored | **1 / 1** | 1 | SUCCESS / absent | 0 |

Cleanup cancels the old local ETA entry, and the restored message is scheduled
again. The old and new timers did not both execute the body. This does not imply
that ETA work is durably stored in a worker or that every outage restores it
immediately; these cases allow cleanup to reach Redis.

### Prefetch reduction: documented behavior is conditional in this checkout

Concurrency 2, multiplier 2, one active late-ACK task, then a cut. Both settings
are explicitly compared:

| Case | Reduction setting | Recorded QoS mode | Initial / reconnect / subsequent QoS | Bodies / backend / ACK | Exit |
| --- | --- | --- | --- | --- | --- |
| `prefetch-reconnect-reduce` | True | `qos_global=False` | **4 / 4 / 4** | Two overlapping bodies complete; SUCCESS; 2 ACK returns | 0 |
| `prefetch-reconnect-unchanged` | False | `qos_global=False` | **4 / 4 / 4** | Two overlapping bodies complete; SUCCESS; 2 ACK returns | 0 |

The enabled case logs that reduction is skipped because of the recorded
per-consumer QoS mode. The source guard checks `qos_global is False`; it also
applies to this Redis setup, not just the RabbitMQ quorum example in its comment.
Therefore **do not assume that the guide's generic prefetch-reduction description
was active for Redis here**. We did not force a different mode or patch the guard.

## 9. Warm shutdown and the original documentation question

TERM arrives about 0.3 s into a 4 s task. Outage cases keep the broker path down
until about 5.3 s; backend connectivity remains healthy.

| Case | Monitoring / fault | Body / backend | ACK returns / errors | Broker after exit | Exit |
| --- | --- | --- | --- | --- | --- |
| `warm-healthy-prefork` | Monitoring enabled; no fault | Completed / SUCCESS | 1 / 0 | Absent | 0 |
| `warm-outage-solo` | Monitoring disabled; broker outage | **Completed / SUCCESS** | **0 / 1** | **Still unacked** | 0 |
| `warm-outage-prefork` | Monitoring disabled; broker outage | **Completed / SUCCESS** | **1 / 0** | Absent after path recovery | 0 |

In the healthy prefork case, a monitoring `worker-offline` event arrived near the
start of warm shutdown, **before the task body completed**. Monitoring heartbeats
had stopped; the task still finished and ACKed. Monitoring offline is not proof
that all task execution has finished.

Solo returned from work and attempted its ACK while the path was still down;
it exited with the delivery unacked. Prefork's ACK completed after the path became
available. Neither case restarted the full task consumer during shutdown.

These results answer the Redis-specific concern: **absence of monitoring
heartbeats does not make Redis terminate a running task**, and losing the broker
connection does not necessarily prevent body completion. It can prevent successful
settlement. Warm shutdown is also not a promise to reconnect and resume normal
consumption or to repair every old delivery before exit.

## Operational guidance for Redis, especially solo

- Decide whether you need Celery monitoring events. `--without-heartbeat` can be
  appropriate when you do not, but it is not a connection-reliability switch.
- Configure and test Redis health checks and bounded socket operations for your
  actual Kombu/redis-py versions. **Do not copy these accelerated 1 s/2 s test
  values blindly into production.** Excessively aggressive timeouts cause false
  failures and duplicate work under load.
- Treat retry switches as permission to retry after errors, not watchdogs.
  `socket_connect_timeout` does not bound a connection that established TCP but
  subsequently stopped replying. `socket_timeout` does not bound every poller wait.
- Solo cannot service its consumer loop while executing an ordinary inline task.
  Long-running tasks delay monitoring, network-failure detection, and task intake.
  If responsive supervision is required, use independent execution capacity or a
  separately supervised process rather than relying on the solo event loop.
- Cancellation-on-loss is not universally implementable: threads cannot kill
  the running thread, solo can detect loss too late, and already-ACKed tasks are
  excluded. Enabling it does not undo side effects.
- Separate consumer recovery, body outcome, backend result, and broker state in
  monitoring. A fresh ping proves new consumption—not settlement of old work.
- Use idempotency/business-level deduplication and reconcile important work against
  durable application state. Completed-task deduplication is useful but not a
  distributed execution lock or protection for every delivery-loss window.
- Choose visibility timeout against the **full unacked interval**, including ETA
  and execution waiting. Recovery scans need a live consumer. Raw redelivery does
  not increment Celery retry counters or obey a task's `max_retries` budget.
- Do not assume Redis result DB separation isolates availability: this experiment
  deliberately bypasses the broker fault for the backend. A shared production
  Redis endpoint outage can affect both.

## Reproduce and inspect

```bash
# Existing uv environment and mise-managed private Redis; no new dependencies:
mise run setup
mise exec -- uv run --locked --no-sync python redis-connection/test-redis-fault-proxy.py
mise run redis-connection

# Focused solo/settlement checks:
mise run redis-connection \
  --case monitor-solo --case active-solo-cancel --case outage-solo \
  --case ack-before-commit-dedup --case ack-reply-lost \
  --case delivery-reply-lost
```

- Worker module: `redis-connection/redis_connection_probe.py`.
- Socket fault injector: `redis-connection/redis_fault_proxy.py`.
- Runner: `redis-connection/verify-redis-connections.py`.
- Final [47-case summary](redis-connection/redis-connection-runs/20261114T130305.113800Z/summary.json).
- Each case contains `config.json`, worker logs, `events.jsonl`, `proxy.jsonl`,
  `monitor.jsonl`, and `result.json` with phase snapshots and assertions.
- Final console: `redis-connection/redis-connection-validation-final.log`.
- Five framing tests pass; no safety-deadline kills were needed. The two planned
  SIGKILLs are the explicit crash/replacement cases.

Exploratory artifacts are retained. Two instrumentation issues were corrected:
Redis Pub/Sub also exposes backend notifications across DB numbers, so the monitor
must filter non-Kombu messages; and dropped BRPOP replies can use RESP3 null framing,
so the fault inspector was extended and unit-tested for it. The final full run
uses those corrections. No broker/client library behavior was monkey-patched;
observation wrappers delegate to the original operations.

## Appendix: mechanisms and interpretation

### A1. A Redis “connection” has several layers

A Celery consumer holds a Kombu connection/blueprint. Its Redis transport uses
multiple redis-py sockets for blocking pops, Pub/Sub control, and ordinary commands.
Monitoring publication can have another Kombu connection. The result backend is
separate again.

Consequently:

- A Redis socket can reconnect without restarting the Celery consumer.
- A publisher can reconnect without repairing the BRPOP path.
- A consumer can restart while an old request/result callback remains alive.
- A healthy result backend says nothing about broker-path availability.

The report records transport sockets, consumer generations, and actual follow-up
tasks rather than treating a single “Connected” log line as proof of all layers.

### A2. Redis ACKs are data-structure operations, not AMQP ACK frames

In this transport, ACK removes the tag from an unacked sorted-set index and hash.
The transaction can use a newly established Redis client socket. There is no AMQP
channel-local delivery-tag validation at the Redis server.

**Restored messages retained their delivery tag in these experiments.** Both an
old request and a redelivered request can therefore refer to the same Redis record.
A successful ACK operation may remove that record or find it already absent;
counting ACK returns does not count distinct successful task bodies. An old
request can also settle bookkeeping while a restored copy is already reserved
or running. This is another reason not to interpret “ACK returned” as exactly-once
execution or as a separate broker acknowledgment for every invocation.

There are two different ambiguity/loss boundaries:

```text
BRPOP succeeded, reply lost before unacked registration
  → no task receipt; no unacked record to recover

Task completed; ACK attempted
  → before Redis commit: record survives, later redelivery possible
  → after Redis commit but reply lost: client sees error, record already gone
```

The ACK/reject distinctions in the earlier task-policy appendix remain valid once
the operation takes effect. This suite adds the network boundary: the caller may
not know whether that happened.

### A3. Reconnect, redelivery, and retry are different operations

```text
Socket error / transport health check
  → socket repair or consumer restart
  → connection policy controls whether to keep trying

Old unacked delivery
  → may be restored during reachable cleanup
  → otherwise may wait for a visibility scan
  → redelivery keeps task ID and retries header

Celery task retry
  → publishes a new task message with incremented retries
  → governed by task retry settings, not connection retry settings
```

A backend RETRY state from cancellation alone does not prove that a replacement
message was published. The suite observes actual task-publication signals and
retry headers to distinguish these paths. Lost-worker settings from the task-policy
report are also not connection-loss recovery settings: a process can stay alive
while its broker connection fails.

### A4. Source references and limits

- `repos/celery/celery/worker/consumer/heart.py` and `repos/celery/celery/worker/heartbeat.py`: optional
  monitoring heartbeat service and stop/offline behavior.
- `repos/celery/celery/worker/loops.py`: AMQP heartbeat support checks and event-loop progress.
- `repos/celery/celery/worker/consumer/consumer.py`: retry selection, loss handling, cancellation,
  old ETA cleanup, and QoS-mode guard for prefetch reduction.
- `repos/celery/celery/worker/request.py`: request-level ACK flag, cancellation, and pool callbacks.
- `repos/celery/celery/app/trace.py`: completed-task deduplication conditions.
- Installed Kombu `transport/redis.py`: BRPOP, health checks, Redis ACK emulation,
  visibility scans, restoration, and client socket options.
- Installed Kombu `transport/virtual/base.py`: unacked registration after receipt
  of a popped message and preservation of serialized delivery metadata.
- `repos/celery/celery/concurrency/prefork.py`: pool draining and timer/ready-callback handling
  during warm shutdown.

These are controlled socket/path failures with a healthy, non-restarting Redis
server and independent backend reachability. They do not establish behavior for
Redis data loss, failover, TLS, packet-level TCP keepalive, distributed-clock jumps,
production load, arbitrary application broker calls, or other dependency versions.
RabbitMQ's protocol-heartbeat and channel-ACK behavior remains a separate future
experiment, not an extrapolated Redis result.

### A5. Can connection retries guarantee ACK of a completed solo task?

**Consumer reconnection does not replay every failed ACK.** A surviving unacked
record can later be restored and the task can execute again. In the measured solo
outage path:

```text
Task stores SUCCESS
  → Request schedules ACK and sets Request.acknowledged=True
  → Redis ACK fails; ack_log_error logs the connection error
  → consumer reconnects later
  → that failed ACK is not automatically replayed
```

The following settings allow the **consumer** to retry connection establishment
indefinitely; they do not create an ACK retry queue:

```python
broker_connection_retry_on_startup = True
broker_connection_retry = True
broker_connection_max_retries = None
```

There is no configuration-only guarantee in this checkout that a completed task's
ACK will be retried until confirmed. Connection retries also cannot progress while
an operation remains blocked without producing an error.

#### Protecting against repeated execution after SUCCESS

For the completed-task case, consider:

```python
task_acks_late = True
worker_deduplicate_successful_tasks = True
```

With a persistent result backend retaining SUCCESS, Celery can receive the
restored message, **skip its already-completed body, and ACK that delivery**.
The `ack-before-commit-dedup` case demonstrated exactly this: **two receipts, one
body execution**. This recovers through redelivery; it does not replay the original
failed ACK callback.

Requirements and limits:

- The task ID must match, late acknowledgment must be enabled, and the message
  must be marked redelivered.
- SUCCESS must remain available when redelivery occurs. Result retention needs
  to cover the possible recovery delay; a result backend that does not support
  the required persistence is not equivalent.
- Deduplication does not prevent overlapping executions before SUCCESS is stored.
- It is not an exactly-once guarantee. Critical side effects still need
  business-level idempotency—for example, an operation key recorded atomically
  with the corresponding database changes where possible, or an external API's
  idempotency-key mechanism.

#### What ACK-specific retries would require

ACK-specific retry behavior would require implementation work rather than merely
turning on consumer retries. It must account for consumer restarts, shutdown,
ambiguous ACK results, and intervening redelivery without blocking solo's event
loop indefinitely.

Blindly retrying the old Redis tag is problematic: restored deliveries retained
that tag in these experiments, so an old request can affect bookkeeping for a
redelivered copy. Manually deleting unacked records is not a safe substitute for
a delivery-aware acknowledgment design.

Even a correct retry mechanism cannot undo a duplicate that has already started.
Task effects, backend result storage, and broker ACK are separate operations;
retrying connections does not make them atomic.

Relevant source: `repos/celery/celery/worker/request.py` (`acknowledge`),
`repos/celery/celery/worker/consumer/consumer.py` (`call_soon_ack` and request ACK promises),
and installed Kombu `message.py` (`ack_log_error`). The observed Redis connection
error was logged/handled; not every exception type follows that exact path—for
example, `BrokenPipeError` is explicitly re-raised by `ack_log_error`.

### A6. What does `broker_channel_error_retry` change?

This setting, disabled by default, widens the error classes handled by the
**consumer's connection-recovery block**:

```python
# broker_channel_error_retry=False:
recoverable_errors = connection_errors

# broker_channel_error_retry=True:
recoverable_errors = connection_errors + channel_errors
```

Although its documentation mentions AMQP, the shared consumer implementation also
applies to Redis. In the installed Redis transport:

| Error category | Examples | Does consumer recovery require enabling this setting? |
| --- | --- | --- |
| Connection errors | Redis `ConnectionError`, `TimeoutError` | **No**: already included in connection recovery |
| Channel errors | Redis `ResponseError`, `InvalidResponse`, `DataError` | **Yes**, when the error propagates to this recovery block |

For an established connection, recovery still requires
`broker_connection_retry=True`. Initial connection handling uses the startup
retry setting. An error already handled inside an ACK callback may never reach
the consumer's recovery block at all.

The setting does **not**:

- Replay every failed ACK.
- Turn consumer recovery into a task retry.
- Detect failures while solo's event loop is blocked.
- Fix a persistent error such as a Redis key having the wrong type; reconnecting
  can simply encounter the same error again and lead to restart loops.

Therefore enabling it does **not repair the failed ACK in the measured solo
outage case**: that error was already a connection-error category, and reconnecting
the consumer did not replay the ACK.

This explanation is **source-verified, not a new runtime comparison**. The 47-case
suite did not toggle `broker_channel_error_retry`. Relevant source:
`repos/celery/celery/worker/consumer/consumer.py` (`Consumer.start`) and installed Kombu
`transport/redis.py` (`get_redis_error_classes`).

These notes are maintained with the report and will not be overwritten by future
experiment runs.

### A7. Follow-up: which Redis visibility settings actually apply?

The [Redis visibility report](redis-visibility.md) adds **27 runtime
cases**. Only `broker_transport_options['visibility_timeout']` controls delivery
recovery in this checkout. The similarly named backend and top-level values do
not override it; all presence/absence combinations and conflicting values were
tested explicitly.

It also clarifies that the shortest value is not negotiated by Redis: a worker
scans shared unacked records using its own timeout, even for another queue.
A publisher alone does not install that minimum. Solo's deferred early ACK can
still permit overlapping execution when another worker restores the message.

Unlike the ETA reconnect cases above, visibility restoration does not clear old
local ETA timers. One solo worker accumulated and executed multiple copies with
healthy connections. Late-ACK successful-task deduplication skipped completed
copies; the early-ACK control did not. The report separates these new measurements
from reused shutdown, ETA/prefetch, and task-policy evidence.

### A8. Why broker and backend socket settings use inconsistent configuration paths

The broker and result backend use **separate Redis client stacks**, even when
configured with the same Redis server or URL. Kombu implements the broker transport;
Celery's `RedisBackend` implements result storage. Their configuration interfaces
are not symmetrical:

| Purpose | Broker: key inside `broker_transport_options` | Result backend: top-level Celery setting |
| --- | --- | --- |
| Connect timeout | `socket_connect_timeout` | `redis_socket_connect_timeout` |
| Socket operation timeout | `socket_timeout` | `redis_socket_timeout` |
| Health-check interval | `health_check_interval` | `redis_backend_health_check_interval` |
| TCP keepalive | `socket_keepalive` | `redis_socket_keepalive` |

Thus the two groups in the [solo setup](solo-redis.md) are **not duplicate settings
for one connection**. Neither group inherits these values from the other. Matching
values are an illustrative starting point, not a requirement: broker and backend
budgets can differ. They do not configure Redis clients created by application task
code, and a matching health interval does not imply identical scheduling/detection
behavior in the broker loop and backend client.

#### Why not put everything in `result_backend_transport_options`?

Despite its name and the broad documentation description, this dictionary is **not
a general pass-through of redis-py connection arguments**. In this checkout,
`RedisBackend.__init__` explicitly reads the four top-level settings above and
constructs its connection parameters. It does not merge the entire backend transport
options dictionary into those parameters.

Putting `socket_connect_timeout`, `socket_timeout`, `health_check_interval`, or
`socket_keepalive` inside `result_backend_transport_options` therefore does **not**
configure those backend connection parameters. If the top-level settings are omitted,
their configured defaults remain applicable; moving the keys into the dictionary
is not an equivalent configuration.

The dictionary does have selectively implemented options, including:

- `retry_policy`: RedisBackend's retry-helper policy, not a socket timeout or an
  unconditional retry wrapper around every backend operation.
- `global_keyprefix`: result-key prefix, distinct from the broker transport's prefix.
- `result_chord_ordered`: chord result ordering.
- `additional_connection_errors`: additional backend connection-error classification.
- Sentinel-specific `master_name`, `min_other_sentinels`, and `sentinel_kwargs`.

These supported keys do not imply that arbitrary Redis client keys are supported.
Likewise, a backend `visibility_timeout` entry does not configure broker recovery;
see A7 and the [visibility/source audit](redis-visibility.md).

**Placement rule:** use `broker_transport_options` for supported Kombu broker
options, the dedicated top-level `redis_*` settings for backend socket/health
configuration, and `result_backend_transport_options` only for options explicitly
consumed by the selected backend. Configure before backend initialization. Redis
backend URL query parameters are another supported input path and can override
constructed connection parameters, so inspect the URL too when diagnosing effective
settings.

This appendix is **source-backed and configuration-checked, not a new network-fault
experiment**. A connection-free check used distinct broker and backend values and
confirmed that conflicting socket/health entries in the backend transport dictionary
left `app.backend.connparams` unchanged, while its `retry_policy` did take effect.
The original 47-case suite was not rerun for this explanation.

Relevant source: [RedisBackend](repos/celery/celery/backends/redis.py)
(`__init__`, `_params_from_url`, `retry_policy`, `_transport_options`, Sentinel helpers),
[base key/value backend](repos/celery/celery/backends/base.py) (result-key prefix), and installed
Kombu `transport/redis.py` (broker transport options). These are implementation-specific
configuration paths, not interchangeable names inferred from the word “transport”.
