# Celery Redis experiments

Real-worker experiments investigating bugs, gotchas, and inconsistencies between
Celery's documented and actual behavior with **solo, prefork, threads, and gevent**,
plus attempts to harden it without modifying Celery/Kombu sources.

The recorded stack uses **Redis, Celery/Kombu 5.7.0, and macOS ARM64 with prefork
spawn**. Conclusions are checkout/platform-specific; reports retain exact versions
and source hashes. **Passing tests can reproduce bugs—they do not certify correct
behavior or production readiness.**

[Findings](#findings) · [Recommended setups](#recommended-setups-and-hardening) ·
[Suites and reports](#suites-and-reports) · [Run experiments](#running-the-experiments)

## Findings

Each finding has a letter-number ID matching the planning inventory.
Use its Markdown heading link to reference it.

- **Pool:** comma-separated affected scope supported by the evidence, including
  source-backed extrapolation. Comparison entries describe different outcomes per
  pool; an omitted pool is not a safety guarantee.
- **Type:** `bug`, `doc mismatch`, `limitation`, or `surprising semantics`.
  A limitation is an unsupported capability/guarantee, not necessarily a defect.
- **Proofs:** the evidence or reasoning supporting the finding, as defined below.
  Worker proofs name tested pools when narrower than the affected scope; mixed
  proofs do not mean every clause was separately fault-injected.

Proof categories:

- **Reproduced with workers:** observed using real Celery workers and private Redis.
- **Source backed:** traced in the recorded Celery/Kombu implementation; this can
  support shared-code scope beyond the measured pool combinations.
- **Design limitation:** architectural reasoning about guarantees, not an additional
  experiment or a claim that an application recovery system was implemented/tested.
- **Unit check:** isolated Python tests, often with mocks or an in-memory broker,
  without real workers or Redis; for example, checking a wrapper's exception handling.
- **Connection-free configuration check:** construct app/transport/backend objects
  and inspect their effective settings **without opening Redis connections**; for
  example, check whether a setting reaches `app.backend.connparams`. This verifies
  configuration wiring, not network-failure behavior.

## An opinionated shortlist

My picks of particularly concerning findings for solo workers with late-ACK tasks and Redis
as the broker:

- [A1 - Solo QUIT can ACK interrupted work](#a1---solo-quit-can-ack-interrupted-work)
- [B1 - Solo early ACK can happen after the body](#b1---solo-early-ack-can-happen-after-the-body)
- [B2 - Reconnect does not replay every failed completion ACK](#b2---reconnect-does-not-replay-every-failed-completion-ack)
- [B4 - Transport gap: a task can disappear between BRPOP and unacked registration](#b4---transport-gap-a-task-can-disappear-between-brpop-and-unacked-registration)
- [C3 - `worker_eta_task_limit` saturation can starve immediate work with idle capacity](#c3---worker_eta_task_limit-saturation-can-starve-immediate-work-with-idle-capacity)
- [D1 - ETA/countdown longer than visibility can execute repeatedly on one healthy worker](#d1---etacountdown-longer-than-visibility-can-execute-repeatedly-on-one-healthy-worker)
- [E1 - Failure ACK False discards ordinary failures rather than retrying them](#e1---failure-ack-false-discards-ordinary-failures-rather-than-retrying-them)
- [G1 - The three documented visibility settings are not three effective timers](#g1---the-three-documented-visibility-settings-are-not-three-effective-timers)

### A - Shutdown and cancellation

#### A1 - Solo QUIT can ACK interrupted work

*Pool: solo · Type: bug · Proofs: reproduced with workers*

Stopping a solo worker with QUIT can leave a task unfinished but remove its message from
the broker, so it will not be retried automatically. Prefer TERM when the task can
finish normally.

The [QUIT requeue
checks](shutdown.md#a8-requeueing-a-running-solo-task-after-the-quit-kill_job-error)
show unsupported `kill_job` becoming an ordinary `NotImplementedError`: default failure
handling ACKs it, while disabling failure ACK discards it. A guarded
`Reject(requeue=True)` restored the original delivery only with late ACK. The [packaged
workaround](solo-redis-hardening.md#3-cold-shutdown-useful-guard-not-a-cancellation-guarantee)
depends on private state and exception text, covers only guarded execution, and cannot
undo effects already performed.

#### A2 - Solo soft-shutdown grace pauses the running task

*Pool: solo · Type: limitation · Proofs: reproduced with workers*

Solo's soft-shutdown grace period does not give its running task extra time to finish.
It pauses that same task while waiting, so increasing the grace period can merely delay
shutdown.

The [shutdown comparison](shutdown.md#bottom-line) shows the handler sleeping in the
same thread that executes the solo task. This differs from prefork, whose child can keep
working while the parent waits; short prefork tasks completed during grace. The
limitation concerns the solo cold-shutdown grace path, not normal TERM-based completion.

#### A3 - Soft shutdown is not a TERM deadline

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers, source backed*

Setting a soft-shutdown timeout does not make a worker exit that many seconds after
TERM. A warm shutdown can still wait indefinitely for a task that never finishes.

The [shutdown lifecycle](shutdown.md#a2-what-is-worker_soft_shutdown_timeout-for) places
`worker_soft_shutdown_timeout` before cold cancellation, not after an ordinary TERM.
Repeated TERM does not escalate warm shutdown, and teardown can continue after the soft
grace expires. Use a separately enforced supervisor deadline if process exit must be
bounded.

#### A4 - Threads cannot forcibly cancel running tasks

*Pool: threads · Type: limitation · Proofs: reproduced with workers*

With the threads pool, Celery cannot forcibly stop one running task. A stuck task can
keep shutdown waiting until the whole worker process is killed.

In the [pool shutdown cases](shutdown.md#bottom-line), cold cancellation raised an
unsupported-operation error in the controller while task threads continued. Warm
shutdown waited for running work, and several hard-escalation cases still required
SIGKILL. Changing shutdown or timeout settings does not add an independent thread-kill
mechanism.

#### A5 - Gevent signal-based shutdown can raise `BlockingSwitchOutError`

*Pool: gevent · Type: bug · Proofs: reproduced with workers*

A gevent worker can fail while handling shutdown instead of following the requested
grace period. Successful normal task execution does not establish that its signal-based
shutdown path is safe.

The [gevent shutdown cases](shutdown.md#bottom-line) raised `BlockingSwitchOutError`
when signal handling attempted operations that require switching greenlets. The same
problem appeared with [enabled idle soft grace](shutdown.md#11-idle-workers). This
OS-signal context differs from ordinary cooperative execution and from cancellation
handled after a broker connection error.

#### A6 - Canceled gevent work can take a success-callback ACK path

*Pool: gevent · Type: bug · Proofs: reproduced with workers, source backed*

Canceling a gevent task does not guarantee that its unfinished work will be retried.
Celery can acknowledge the message even when late acknowledgment is enabled and the task
did not complete.

The [cancellation implementation and outcomes](shutdown.md#why-the-pools-differ) show a
canceled greenlet taking the pool's success-callback ACK path. Cold-shutdown cases left
incomplete work PENDING in the result backend but absent from the broker. Treat the ACK
as removal of that delivery, not evidence that the intended work finished.

#### A7 - Signal escalation is not an uncatchable task kill

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers, source backed*

Repeated shutdown signals are not a reliable way to enforce an exact stop deadline. Task
code can catch some interruptions, and native code can delay when Python handles
signals.

The [exception-handling cases](shutdown.md#8-can-task-code-catch-cancellation)
distinguish catchable cancellation paths, while the [native-code
cases](shutdown.md#9-native-code-delays-and-signal-coalescing) show repeated SIGINTs
coalescing into fewer handler calls. Several Ctrl+C presses therefore need not reach the
hard stage. Celery's hard shutdown remains different from OS SIGKILL; external
supervision must enforce any final process deadline.

#### A8 - Remote-control shutdown cannot preempt busy solo execution

*Pool: solo · Type: limitation · Proofs: reproduced with workers*

A remote shutdown command cannot interrupt a busy solo worker immediately. The worker
has to return to processing control messages first, so remote control is not a
substitute for process supervision.

In the [remote-shutdown
test](shutdown.md#10-remote-control-shutdown-is-not-an-os-signal), solo handled the
broadcast only after its task returned; the other pools could process control while work
continued. The command arrived through Redis rather than as an OS signal, and the test
did not establish any way to stop an endless solo task remotely.

#### A9 - Waiting and running work have different shutdown outcomes

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers, source backed*

Waiting tasks and running tasks can be handled very differently during shutdown.
Successful recovery of one group says nothing about whether the other group was
preserved.

The [mixed-work
cases](eta-prefetch.md#6-running-work-and-waiting-work-must-be-distinguished) restored
waiting ETAs even when the active solo task was ACKed as a failure. The [scheduled-work
cases](eta-prefetch.md#5-shutdown-while-all-work-is-still-scheduled) also show that
ETA-only workers count as idle for soft-grace selection, and enabled idle grace did not
execute work becoming due. SIGKILL skips cleanup, leaving surviving unacked records to
later recovery rather than immediately returning them to the queue.

### B - ACKs, delivery loss, and outcome indicators

#### B1 - Solo early ACK can happen after the body

*Pool: solo · Type: surprising semantics · Proofs: reproduced with workers*

With solo, early acknowledgment does not reliably mean Redis has removed the message
before the task starts. The task can still be delivered elsewhere while its first
execution is running.

The [ACK scheduling
path](shutdown.md#a5-how-are-ack-callbacks-queued-and-how-should-this-work) sets
`Request.acknowledged=True` after queuing a hub callback, while inline execution
prevents that callback from running. The flag can already block rejection even though
Redis still holds an unacked record. A second worker [restored and executed that
delivery](redis-visibility.md#early-ack-is-not-necessarily-early-on-the-redis-socket)
before the first body finished; this is not equivalent to late-ACK semantics.

#### B2 - Reconnect does not replay every failed completion ACK

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers (solo), source backed*

A worker can reconnect and resume normal service without fixing an acknowledgment that
failed earlier. A task that already completed may consequently be delivered and executed
again.

The [failed-ACK
analysis](redis-connection.md#a5-can-connection-retries-guarantee-ack-of-a-completed-solo-task)
records solo storing SUCCESS, failing ACK, reconnecting, running fresh work, and exiting
0 while the old delivery remained unacked. The handled callback failure was not
replayed. Unlimited consumer connection retries or extra shutdown grace do not create an
ACK retry queue; later redelivery with retained success information is a separate
recovery path.

#### B3 - An ACK error does not reveal whether Redis committed it

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo), source backed*

After a connection error during acknowledgment, the message may already be removed from
Redis—or may still need recovery. Treat the outcome as uncertain rather than
automatically repeating the work.

The [wire-fault
tests](redis-connection.md#5-ack-ambiguity-distinguish-redis-commit-from-the-clients-observation)
distinguish failure before the ACK transaction committed from loss of its reply
afterward. The first left an unacked record and allowed redelivery; the second had
already removed the record despite the client reporting an error. Neither exception
counts nor successful ACK-return counts establish how many deliveries Redis actually
settled.

#### B4 - Transport gap: a task can disappear between BRPOP and unacked registration

*Pool: solo, prefork, threads, gevent · Type: bug · Proofs: reproduced with workers (solo), source backed*

Redis-backed tasks can be lost even after successful publication and with late
acknowledgment enabled. There is a brief handoff window where Redis has removed the task
but recovery information has not yet been recorded.

The [lost-delivery
experiment](redis-connection.md#6-delivery-reply-lost-before-unacked-registration)
dropped a message-bearing BRPOP reply before Kombu registered it as unacked, leaving
nothing for reconnect or visibility recovery. Worker death in that interval is a
source-backed risk, not the injected fault. [Kombu PR
#2831](solo-redis-hardening.md#kombu-pr-2831) proposes closing this gap; its retained
2026-11-16 snapshot was open/unmerged, not evidence of an installed or released fix.

#### B5 - Restored Redis deliveries retain tags

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo, prefork), source backed*

Retrying an old acknowledgment is not a safe shortcut for delivery recovery. It can
remove the broker record that a later execution relies on for its own recovery.

The [ACK-retry analysis](redis-connection.md#what-ack-specific-retries-would-require)
explains that restored Redis deliveries retain their delivery tag. An old request and a
newly received copy can therefore address the same unacked bookkeeping. Blind ACK replay
or manual deletion can affect that copy, and multiple successful ACK returns need not
correspond to distinct removals.

#### B6 - Task completion hooks are not confirmed-ACK hooks

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: source backed*

A task lifecycle hook is not confirmation that the broker has removed the message.
Wrapping task completion is therefore not a reliable place to catch and retry
acknowledgment failures.

The [hook
audit](solo-redis-hardening.md#5-completion-and-ack-recover-safely-instead-of-adding-a-blind-retry-loop)
traces `Request.acknowledge()` through callback scheduling to the later Redis operation.
`on_success` and `after_return` do not confirm settlement, and wrapping the Request
method may see only successful scheduling. `ack_log_error` handles some failures but
re-raises `BrokenPipeError`; the hardening guide deliberately supplies no blind old-tag
ACK retry loop.

#### B7 - Body effects, backend state, ACK, and process exit are independent

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers, source backed*

“Task succeeded,” “message acknowledged,” and “worker exited cleanly” describe different
things. None alone proves that the business operation ran exactly once or cannot run
again.

The [shutdown outcomes](shutdown.md#bottom-line) include SUCCESS with restored work,
PENDING with ACKed incomplete work, and RETRY without a published retry. The
[warm-outage
cases](redis-connection.md#9-warm-shutdown-and-the-original-documentation-question) add
exit 0 with failed settlement. Business effects, backend writes, ACKs, and teardown are
separate operations; cancellation does not roll back an effect that already committed.

#### B8 - Successful-task deduplication is not an execution lock

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers (solo, prefork), source backed*

Celery's successful-task deduplication can skip a repeat after completion; it does not
stop two copies from running at the same time. Important side effects still need
application-level idempotency.

The [deduplication checks and
gates](redis-connection.md#protecting-against-repeated-execution-after-success) require
the relevant late-ACK policy, matching task ID, redelivery metadata, and available
success information. They demonstrated two receipts with one body execution after
SUCCESS, but concurrent copies can both enter before SUCCESS exists. Early-ACK ETA
repeats and independently republished same-ID messages do not automatically qualify.

### C - ETA/countdown, prefetch, and saturation

#### C1 - Waiting ETAs occupy memory/timers, not execution slots

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers*

A worker can hold many future tasks without using its execution slots. Those tasks still
consume worker memory and have not yet been acknowledged to the broker.

The [four-pool storage tests](eta-prefetch.md#1-storage-is-not-execution-all-four-pools)
held five future tasks while immediate work still executed. Waiting ETAs occupied timer
entries and remained unacked even under early-ACK policy. The [ordinary-prefetch
controls](eta-prefetch.md#3-ordinary-prefetch-fetching-is-not-acknowledging) distinguish
receipt, reservation, execution acceptance, and ACK: prefetch is not simply a count of
currently running tasks.

#### C2 - Prefetch 1 and `worker_disable_prefetch=True` do not cap ETA intake

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (all four for ETA storage; prefork for disabling prefetch), source backed*

Prefetch 1 or disabling normal prefetch does not mean a worker will hold only one
scheduled task. ETA/countdown work needs its own intake policy.

The [ETA admission cases](eta-prefetch.md#4-eta-limits-and-prefetch-interact) show
future-message receipt increasing QoS delivery credit. Prefetch 1 did not stop the
five-message scheduled backlog, and `worker_disable_prefetch=True` also admitted it in
the prefork check. This is shared consumer accounting, not evidence that those options
provide a scheduled-memory bound.

#### C3 - `worker_eta_task_limit` saturation can starve immediate work with idle capacity

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers (prefork), source backed*

An ETA limit can leave a worker idle while immediate jobs remain queued. Delayed retries
can fill the same limit, so limiting scheduled work does not by itself preserve
responsiveness.

The [saturation cases](eta-prefetch.md#issue-1-waiting-eta-tasks-exhaust-delivery-credit) show
`worker_eta_task_limit` acting through shared QoS credit, with active late-ACK work also
consuming it. Once saturated, the worker could not fetch immediate work despite idle
execution capacity. The [hardening
alternative](solo-redis-hardening.md#4-prevent-eta-saturation-at-the-producer-boundary)
keeps delays in application storage and publishes immediate work when due, rather than
relying on the cap as an independent ETA quota.

#### C4 - The ETA cap is not unconditional

*Pool: solo, prefork, threads, gevent · Type: doc mismatch · Proofs: reproduced with workers (prefork), source backed*

With `worker_prefetch_multiplier=0`, `worker_eta_task_limit` does not cap ETA intake
in this stack: zero means unlimited prefetch, not zero tasks. A positive initial
prefetch above the ETA limit is not reduced to that limit either.

The [cap implementation and
cases](eta-prefetch.md#what-the-limit-implementation-actually-does) show prefetch 0
with an ETA limit of 3 admitting all five future tasks. Kombu leaves the zero/unlimited
allowance unchanged instead of applying the ETA cap. Separately, initial
prefetch 8 with an ETA limit of 3 admitted six immediate late-ACK tasks: the initial
window was not clamped. These are cap-enforcement exceptions, not saturation or
baseline drift. Prefetch 0 is also distinct from `worker_disable_prefetch=True`.

#### C5 - Draining a saturated ETA cap can reduce the prefetch baseline

*Pool: solo, prefork, threads, gevent · Type: bug · Proofs: reproduced with workers (prefork), source backed*

After a scheduled backlog drains, a worker can fetch fewer tasks in advance than its
prefetch setting suggests. The scheduling limit can therefore affect ordinary work even
after the delayed tasks have finished.

The [QoS accounting check](eta-prefetch.md#issue-2-draining-scheduled-work-lowers-the-prefetch-baseline)
measured a baseline of 2 rising to the cap of 3 and then falling to 1. Saturated
increments are discarded, but corresponding due-time decrements still occur. This is an
accounting defect in the recorded implementation, not a throughput benchmark or an
intentional prefetch-tuning strategy.

#### C6 - ETA is an earliest start, not a promised start

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers, source backed*

ETA and countdown mean “not before,” not “start exactly at.” A busy worker or a task
rate limit can delay execution beyond the requested time.

The [storage/execution
boundary](eta-prefetch.md#1-storage-is-not-execution-all-four-pools) separates
scheduling a local timer from acquiring execution capacity. The source-backed
[rate-limit
paths](eta-prefetch.md#a2-when-can-the-event-loop-run-before-the-task-body-executes) can
defer a due task until a token is available; those rate-limit combinations were not
additional runtime cases. A countdown reserves neither a pool slot nor a guaranteed
start time.

#### C7 - Solo ETA dispatch runs inline before normal QoS decrement

*Pool: solo · Type: surprising semantics · Proofs: reproduced with workers, source backed*

Solo does not return to worker housekeeping while a task is running, even if the task
sleeps or waits for I/O. Scheduled tasks behave the same way once execution begins.

The [ETA dispatch
trace](eta-prefetch.md#appendix-deterministic-solo-execution-and-event-loop-ordering)
shows `apply_eta_task()` calling the task inline and decrementing QoS compensation only
after execution returns; applying the updated QoS happens later. Releasing the GIL does
not yield to Celery's event loop. The source distinguishes rate-limited ordering and
escaping exceptions that can bypass the subsequent decrement.

### D - Visibility and duplicate execution

#### D1 - ETA/countdown longer than visibility can execute repeatedly on one healthy worker

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo for visibility duplicates; solo, gevent for reconnect cleanup), source backed*

Scheduling a task farther ahead than Redis's visibility timeout can cause redelivery
before it even starts. Multiple executions can then occur without a worker crash or
connection failure.

The [ETA expiry
cases](redis-visibility.md#8-eta-expiry-one-solo-worker-can-accumulate-and-execute-copies)
show visibility restoration leaving the original local timer intact, so retained copies
could each execute at due time; exact counts depend on timing and deduplication policy.
In contrast, the [reconnect tests](redis-connection.md#waiting-eta) cleared the old
timer during cleanup. Visibility scanning and reconnect cleanup are different recovery
mechanisms.

#### D2 - A waiting task can be requeued while its original copy remains in the worker

*Pool: prefork · Type: surprising semantics · Proofs: reproduced with workers*

A task can remain waiting inside a worker while its message is also put back into
Redis's queue. The worker's original copy is not removed or canceled: it is still
waiting to execute when a slot becomes available. The requeued message can be delivered
again, so both copies can run even if the task itself is quick.

In the [reserved-work
experiment](redis-visibility.md#9-prefetched-work-can-expire-before-its-first-execution),
prefork had one execution slot and prefetch 2; a short ordinary task waited behind a
long-running task, without ETA scheduling. After `visibility_timeout`, a consumer
recovery scan restored both messages from Redis's unacked bookkeeping to the queue,
without removing the worker's waiting request or stopping its running task. The worker
processed the original and redelivered copies, so each task executed twice. Visibility
must therefore cover local waiting time as well as execution.

#### D3 - Visibility measures registration age, not a renewable execution lease

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo, prefork), source backed*

Redis does not keep extending a task's visibility timeout because the task is still
alive. Long-running work can be redelivered while its original execution continues.

The [visibility lifecycle](redis-visibility.md#2-what-the-timeout-actually-measures)
starts the clock at unacked registration, including ETA, pool and rate-limit waits,
execution, and settlement delays—not time spent queued before receipt. Monitoring does
not renew that timestamp. Expiry only makes the record eligible for restoration; it
neither cancels the original task nor automatically rejects a later ACK.

#### D4 - Expiry requires a scanner, not a Redis TTL

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo), source backed*

Passing the visibility timeout does not immediately put a task back on the queue.
Recovery needs a consumer to scan for abandoned deliveries, so actual redelivery can
happen later.

The [recovery-timing
cases](redis-visibility.md#6-recovery-needs-a-scanning-actor-not-just-an-expired-age)
left expired records unacked while no consumer scanned them. Scan interval, throttling,
locks, and event-loop progress add delay; the installed async defaults imply roughly 100
seconds between steady-state scans. That cadence is source-derived, not a 100-second
measurement, and the exact gevent cadence is not inferred from it.

#### D5 - “The shortest visibility wins” is not server negotiation

*Pool: solo, prefork, threads, gevent · Type: doc mismatch · Proofs: reproduced with workers (solo scanners, with solo/prefork owners), source backed*

Workers sharing Redis recovery records can apply different timeouts to the same tasks. A
worker on another queue can therefore cause redelivery sooner than the task's own worker
expects.

The [scanner-scope
checks](redis-visibility.md#the-shortest-value-wins-is-not-server-side-negotiation) show
each active consumer applying its own visibility value to shared unacked records,
including records for queues it does not consume. Redis negotiates no minimum, and a
publisher alone sets none. Queue names did not isolate recovery policy; a distinct
broker key prefix did in the measured case.

#### D6 - Solo's blocked scanner does not prevent duplicates

*Pool: solo · Type: surprising semantics · Proofs: reproduced with workers*

Running one task at a time does not stop another worker from running a copy of the same
job. Solo's lack of local recovery activity while busy offers no protection against
other workers scanning Redis.

The [mixed-worker cases](redis-visibility.md#7-solo-execution-and-mixed-worker-scope)
contrast a lone solo worker, which ran past visibility without scanning, with a second
worker that restored and executed its delivery concurrently. This also occurred with
deferred early ACK. Shortening visibility can improve abandoned-work recovery while
increasing the chance of duplicating still-running work.

#### D7 - Result retention is a different clock

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo), source backed*

An expired result does not mean the task never ran, and it does not make the broker
resend the task. Result retention and delivery recovery must be configured separately.

The [result-expiry check](redis-visibility.md#10-result-retention-is-a-different-clock)
observed `result_expires` removing a completed task's SUCCESS key and exposing PENDING
without any redelivery. Backend/top-level visibility values did not set that TTL.
Retention must independently cover the period in which completed results are needed,
including eligible successful-task deduplication.

### E - Failure, timeout, and worker-loss policies

#### E1 - Failure ACK False discards ordinary failures rather than retrying them

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers*

Turning off failure acknowledgment does not mean “keep failed tasks for retry.” For
ordinary late-ACK failures, Celery discards the delivery unless a different recovery
path is explicitly requested.

The [failure-policy
cases](task-policy.md#1-ordinary-task-failure-policy-works-in-every-pool) show
`task_acks_on_failure=False` producing `Reject(requeue=False)` and removing Redis's
unacked records, rather than leaving them for visibility recovery. Early-ACK tasks are
different: later failure flags cannot undo their acknowledgment. Use explicit
retry/requeue policy instead of treating this flag as an automatic retry switch.

#### E2 - Soft limits were enforced only by prefork

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers*

Soft time limits are not portable across pools. Even where supported, they are catchable
exceptions rather than guaranteed execution deadlines.

The [soft-limit
cases](task-policy.md#2-soft-time-limit-enforced-only-by-prefork-among-these-pools)
enforced the deadline only with prefork. An unhandled `SoftTimeLimitExceeded` followed
ordinary failure policy, not the hard-timeout ACK setting. Catching it and returning
reported SUCCESS despite an incomplete workload, while catching it and explicitly
retrying published another attempt.

#### E3 - Hard limits and timeout disposition depend on pool and exception class

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers*

A configured hard time limit may not be enforced by your pool. Where timeout handling
does run, requeueing can repeat the task indefinitely rather than obeying its retry
budget.

The [hard-limit cases](task-policy.md#3-hard-time-limit-and-task_acks_on_timeout)
enforced limits with prefork and cooperative gevent, but not solo or threads. Disabling
timeout ACK raw-requeued with an unchanged retry counter, so `max_retries` did not cap
it. Hard timeout is not worker loss, a generic `TimeoutError` is not
`TimeLimitExceeded`, and manually raising a timeout exception tests handling rather than
enforcement.

#### E4 - Gevent's overdue timeout can fire after completion and SUCCESS

*Pool: gevent · Type: bug · Proofs: reproduced with workers*

A gevent task can finish its work and still be timed out afterward. If timeout policy
requeues it, already-completed effects can be repeated.

In the [delayed-timeout
cases](task-policy.md#gevent-delayed-timeout-can-duplicate-completed-work), a
non-yielding body ran beyond its deadline before the overdue callback fired during
result I/O. Redis had already stored SUCCESS; timeout requeue then produced another
completed execution. The backend preserved SUCCESS even though a timeout failure hook
ran, showing why result state alone cannot describe the delivery outcome.

#### E5 - Worker-loss reject and retry are different mechanisms

*Pool: prefork · Type: surprising semantics · Proofs: reproduced with workers, source backed*

Returning a lost task to the broker and publishing a Celery retry have different limits
and acknowledgment requirements. Choose deliberately rather than treating the
worker-loss flags as interchangeable.

The [prefork child-loss
cases](task-policy.md#4-actual-prefork-child-loss-reject-vs-retry) show reject requiring
late ACK and preserving the retry counter; retry publishes a new same-ID message,
increments the counter, and can work after early ACK. Retry takes precedence, and
exhaustion suppresses raw-requeue fallback. Retrievable SUCCESS can suppress repetition;
failed retry-publication fallback was source-checked, not fault-injected.

#### E6 - A dead solo worker cannot handle its own loss

*Pool: solo · Type: limitation · Proofs: reproduced with workers, design limitation*

With solo, the task and the worker's recovery logic die together. Worker-loss flags
cannot publish a retry from a process that no longer exists.

The [actual process-loss
case](task-policy.md#5-solo-a-dead-worker-cannot-handle-its-own-loss) enabled both flags
before `os._exit`, but left only an unacked delivery for later broker recovery. Manually
raising `WorkerLostError` exercised a surviving handler and is not equivalent to process
death. Solo QUIT's `NotImplementedError` follows ordinary failure handling, not a
worker-loss or timeout branch.

#### E7 - Non-prefork `SystemExit` reaches an incompatible callback

*Pool: solo, threads, gevent · Type: bug · Proofs: reproduced with workers, source backed*

Calling `sys.exit()` inside a task is not a safe way to fail it or request a retry.
Outside prefork, it can terminate the worker or leave it unable to accept more work.

The [SystemExit
cases](task-policy.md#6-systemexit-is-a-distinct-failure-pathand-exposes-a-defect)
passed `ExceptionInfo` to a callback expecting a tuple. Solo exited through cleanup;
threads/gevent stranded credit and stalled follow-up work at prefetch 1. Prefetch 2 hid
the complete stall without repairing the delivery. The [solo
guard](solo-redis-hardening.md#plain-systemexit-guard-and-failure-policy) converts plain
SystemExit to ordinary failure while preserving worker-control exceptions.

### F - Connections and monitoring

#### F1 - Monitoring heartbeats are not Redis protocol heartbeats

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers, source backed*

Celery's monitoring events and Redis connection health are different mechanisms.
Disabling monitoring heartbeats does not disable reconnect, and missing heartbeats do
not mean Redis killed a task.

The [heartbeat checks](redis-connection.md#1-monitoring-heartbeats-while-tasks-execute)
observed Redis negotiating no protocol heartbeat, while recovery from socket closes
still worked with `--without-heartbeat`. Monitoring paused during solo and non-yielding
gevent bodies without Redis terminating them for that absence. Redis health checks and
Celery monitoring events must therefore be configured and interpreted separately.

#### F2 - Solo and non-yielding gevent can stall the control plane

*Pool: solo, gevent · Type: limitation · Proofs: reproduced with workers*

A worker can be alive and running your task while appearing unresponsive to pings or
monitoring. With solo or non-cooperative gevent work, those functions compete with task
execution for progress.

The [loop-progress
observations](redis-connection.md#1-monitoring-heartbeats-while-tasks-execute) show
monitoring pausing while these bodies occupy their execution loop. For solo, even sleep
or blocking I/O does not return control to Celery; connection-loss handling and health
checks can also wait. A short ping/heartbeat deadline alone is not reliable evidence
that the worker or its business operation has died.

#### F3 - Reconnect cannot repair a failure not yet detected

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers (solo, prefork, gevent; no-health control in solo), source backed*

A stuck connection is not always a detected connection error. Reconnect policies cannot
help until Celery notices the problem, and a socket timeout alone may not cover every
wait.

The [silent-BRPOP tests](redis-connection.md#brpop-response-never-arrives) blackholed a
socket whose async poller never reached a blocking read. `socket_timeout` alone did not
recover it in the observation window; the enabled transport watchdog did when loop
progress allowed it. The watchdog is specific to the installed Kombu implementation and
must not be assumed present in older versions.

#### F4 - Monitoring publication can block the controller

*Pool: solo · Type: limitation · Proofs: reproduced with workers*

Monitoring itself can make a solo worker stop accepting work if publishing an event to
Redis hangs. Monitoring traffic needs bounded I/O just like task traffic.

The [silent-monitoring-publisher
cases](redis-connection.md#synchronous-monitoring-publication-never-returns) blocked
synchronous publication without breaking the consuming sockets. With no socket timeout,
dispatch and health checks stopped until manual rescue; a bounded timeout allowed
progress without rescue. Retry layers can still extend the surrounding operation beyond
one socket timeout.

#### F5 - Startup and runtime reconnect switches are independent

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers (solo), source backed*

A worker's policy for starting without Redis is separate from its policy for losing
Redis after startup. The retry budget here counts connection attempts, not task retries.

The [connection-policy
cases](redis-connection.md#startup-and-runtime-are-separate-decisions) distinguish
`broker_connection_retry_on_startup` from `broker_connection_retry` and show initial
retry exhaustion or runtime exit when disabled. The unset startup fallback was
source-checked. An unlimited connection budget supplies neither an ACK replay queue nor
a total wall-clock limit for an operation.

#### F6 - Cancel-on-loss is not portable task cancellation

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers, source backed*

“Cancel tasks when the broker connection is lost” does not mean every pool can stop
running work. The setting also does not wait until a task has run for a particular
duration.

The [active-task disconnect
cases](redis-connection.md#3-cut-during-an-active-task-timing-and-cancellation-matter)
show `worker_cancel_long_running_tasks_on_connection_loss` selecting unacknowledged
late-ACK requests: prefork canceled, threads raised unsupported cancellation, and solo
handled loss after its body returned in that window. Gevent's ordinary error handler
could cancel, unlike its problematic OS-signal path, but still produced an ACK for
canceled work. There is no separate “long-running” duration threshold.

#### F7 - Reconnect can restore a task while its original body continues

*Pool: prefork, threads, gevent · Type: surprising semantics · Proofs: reproduced with workers*

With these pools, connection recovery can make a task available again while its original
execution is still running. Without effective cancellation, this can produce duplicate
executions rather than simply resume processing.

The [active-work disconnect
tests](redis-connection.md#3-cut-during-an-active-task-timing-and-cancellation-matter)
completed duplicate bodies where cancellation was disabled or ineffective; with enough
capacity, copies can overlap. In contrast, solo's consumer handled reconnection only
after its inline body returned. Its ACK succeeded before consumer recovery in the brief
cut, but failed during the sustained outage, leaving completed work unacked—the
[B2](#b2---reconnect-does-not-replay-every-failed-completion-ack) problem. Solo overlap
caused by another worker's visibility scan is covered by
[D6](#d6---solos-blocked-scanner-does-not-prevent-duplicates), not by consumer reconnection
during the original body.

#### F8 - Generic reconnect-prefetch reduction did not apply here

*Pool: solo, prefork, threads, gevent · Type: doc mismatch · Proofs: reproduced with workers (prefork), source backed*

Do not assume that enabling prefetch reduction limits how much work a Redis worker takes
after reconnecting. The setting is bypassed for the Redis setup covered here.

The [reconnect-prefetch
comparison](redis-connection.md#prefetch-reduction-documented-behavior-is-conditional-in-this-checkout)
kept the allowance at 4 → 4 → 4 with `worker_enable_prefetch_count_reduction` both
enabled and disabled. The shared consumer guard skips reduction when `qos_global=False`,
which applied to this Redis setup. The runtime comparison used prefork; broader scope
follows the source guard, not an additional all-pool matrix.

#### F9 - Monitoring offline does not mean all work finished

*Pool: solo, prefork · Type: surprising semantics · Proofs: reproduced with workers*

An “offline” worker event does not prove that the worker has stopped running tasks.
During graceful shutdown, tasks may still be finishing and attempting to acknowledge
their messages.

The [warm-shutdown
cases](redis-connection.md#9-warm-shutdown-and-the-original-documentation-question)
observed prefork emitting offline before body completion. During a broker outage, solo
completed but exited with failed settlement, while prefork's later callback settled
after recovery. Neither restarted the full task consumer during shutdown, so warm exit
is not a reconnect-and-drain guarantee.

#### F10 - Channel-error retry is not general delivery repair

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: source backed*

Retrying channel errors broadens which failures trigger reconnect; it does not repair
every delivery failure. It can also repeat permanent configuration or Redis data errors.

The [source audit of error
classification](redis-connection.md#a6-what-does-broker_channel_error_retry-change)
shows `broker_channel_error_retry` adding propagated channel errors to consumer
recovery. Redis connection errors already qualify without it. Errors handled inside ACK
callbacks may never reach that block, and reconnect does not fix persistent WRONGTYPE
errors or detect blocked I/O. This flag was not runtime-toggled in the fault suite.

### G - Configuration and documentation inconsistencies

#### G1 - The three documented visibility settings are not three effective timers

*Pool: solo, prefork, threads, gevent · Type: doc mismatch · Proofs: reproduced with workers (solo configuration matrix), unit check, connection-free configuration check, source backed*

Redis delivery recovery uses the broker's visibility setting, not three independent
timers that must be kept in sync. Similarly named backend and top-level values do not
configure additional recovery timers.

The [configuration
matrix](redis-visibility.md#1-direct-answer-these-are-not-three-effective-redis-visibility-timers)
found only `broker_transport_options['visibility_timeout']` independently effective. The
[history
audit](redis-visibility.md#a6-git-blame-and-why-the-documentation-says-to-set-all-three)
traced the three-setting recipe through documentation changes, not independent core
wiring. [Aliased dictionaries or explicit application
forwarding](redis-visibility.md#a7-important-qualification-indirect-configuration-effects-are-possible)
can still change the broker value indirectly, so apparent effects from another
configuration location need that distinction.

#### G2 - Broker and backend socket settings use different configuration paths

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: connection-free configuration check, source backed*

The broker and result backend can use the same Redis server without sharing connection
settings. Putting a timeout in the wrong configuration location can leave the actual
connection timeout unchanged.

The [connection-free configuration
check](redis-connection.md#a8-why-broker-and-backend-socket-settings-use-inconsistent-configuration-paths)
distinguishes broker transport `socket_timeout` from backend `redis_socket_timeout`. The
backend does not forward arbitrary socket/health keys from
`result_backend_transport_options`; it selectively consumes supported options. URL
parameters can override constructed connection values, and initialized clients cache
configuration, so inspect resolved settings before assuming a change took effect.

#### G3 - Generic backend retry False does not disable every Redis retry layer

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: source backed*

Disabling one backend-retry switch does not make every Redis operation fail immediately.
Different layers have their own retries and timeouts, so the total delay needs a
separate budget.

The [backend retry guidance](solo-redis.md#why-these-choicesand-their-limits)
distinguishes `result_backend_always_retry` from the Redis helper's retry policy, which
also does not wrap every read. Socket bounds, result-backend retries, publication
retries, and consumer reconnect govern different operations. Turning one off or choosing
a finite socket timeout does not impose a universal wall-clock deadline.

#### G4 - Effective task attributes and defaults matter more than configuration intent

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: connection-free configuration check, source backed*

An application-wide setting is not proof that every task uses it. Task overrides,
fallback defaults, and the installed implementation can change the effective behavior.

The [recorded defaults](task-policy.md#version-and-defaults-matter) allow unset
`task_acks_on_failure` / `task_acks_on_timeout` flags to inherit
`task_acks_on_failure_or_timeout`; there is no separate `task_acks_early` switch. The
[hardening checks](solo-redis-hardening.md#baseline-settings-to-retain-or-make-explicit)
therefore inspect resolved task attributes, not only app configuration.
Source/dependency identity matters too: a version string alone does not establish that
two installations contain the same behavior.

### H - Hardening traps and operational limits

#### H1 - Broad autoretry can consume the shutdown guard's Reject

*Pool: solo · Type: surprising semantics · Proofs: unit check, source backed*

A broad autoretry rule can catch the exception used to requeue a task during shutdown. A
cancellation workaround must be composed with the task's existing exception and retry
handlers.

The [guard audit and
checks](solo-redis-hardening.md#exact-coverage-and-failure-boundaries) require narrow
retry exceptions or `Reject` in `dont_autoretry_for`, so shutdown requeue is not
transformed into publication of another retry. Inner catches can also alter the
exception. The guard covers only errors reaching its decorated body, not setup, result
storage, ACK handling, failed rejection, or abrupt process death.

#### H2 - Publication signals are not reliable veto hooks

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: unit check, source backed*

Raising inside a task-publication signal handler is not a reliable way to veto sending a
task. Enforcing a publication policy requires guarding the publication path and
accounting for all publishers.

The [publication-policy
audit](solo-redis-hardening.md#4-prevent-eta-saturation-at-the-producer-boundary) notes
that `before_task_publish` receiver exceptions can be captured instead of aborting
publication. The supplied app subclass guards its normal send path, not other apps, raw
publishers, or already queued ETAs, and does not prevalidate an entire canvas. A
forbidden callback may fail only when published after its parent has already committed
effects.

#### H3 - Blocking delayed publication can discard a task retry

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: unit check, source backed*

Simply forbidding ETA/countdown messages can break existing task retries. A task trying
to publish a delayed retry can be discarded instead of safely rescheduled.

The [retry-policy
checks](solo-redis-hardening.md#delayed-retries-are-a-particularly-important-migration-trap)
show stock `Task.retry()` converting a publication exception into
`Reject(requeue=False)`. Default retry delay and autoretry backoff also produce native
scheduled messages, even without an explicit ETA in application code. Migrate those
retries to an allowed scheduling path rather than assuming the publication guard
preserves their work.

#### H4 - Immediate retry is not outage backoff

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: reproduced with workers (solo), source backed, design limitation*

Immediate retries may exhaust their budget before an unavailable service recovers.
Avoiding ETA/countdown does not remove the need for backoff or limits on repeated
business operations.

The [immediate-retry check and
guidance](solo-redis-hardening.md#delayed-retries-are-a-particularly-important-migration-trap)
demonstrate explicit countdown 0 with a finite retry budget, not sustained-outage
backoff. `max_retries` counts Celery retries rather than visibility restores, cold
requeues, or independent business republication. Delayed business recovery needs
separately persisted scheduling/attempt policy if native ETA publication is prohibited.

#### H5 - An outbox alone does not repair post-publication delivery loss

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: design limitation*

An outbox can ensure that a recorded job gets published, but publication is not
completion. If delivery is lost afterward, the application still needs a way to detect
and resubmit unfinished business work.

The [measured delivery
gap](#b4---transport-gap-a-task-can-disappear-between-brpop-and-unacked-registration)
motivates the [durable job and reconciliation
patterns](solo-redis-hardening.md#7-minimal-application-patterns-for-the-gaps-wrappers-cannot-close):
retain business intent and completion independently of broker publication. Effects,
backend results, and ACKs are not atomic, and task IDs, cache flags, or expiring locks
do not create exactly-once effects. These are design requirements; no outbox or
business-ledger implementation was tested.

#### H6 - A successful “rescheduled” attempt can suppress needed future work

*Pool: solo, prefork, threads, gevent · Type: surprising semantics · Proofs: source backed, design limitation*

If a task reports success after arranging later work, reusing its task ID can make
Celery treat a future attempt as already completed. Keep business-job identity separate
from individual Celery attempts.

The [rescheduling
design](solo-redis-hardening.md#backend-outage-and-explicit-rescheduling) distinguishes
a successful “rescheduled” receipt from business completion. Reusing its ID can suppress
an eligible redelivery when SUCCESS deduplication applies; it does not mean every fresh
same-ID publication is skipped. Use distinct attempt IDs, retain per-attempt outcomes,
and check `not_before` in the worker. This is source-backed guidance, not a validated
ledger implementation.

#### H7 - Pool tuning does not create missing execution isolation

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: source backed, design limitation*

Configuration cannot give a pool execution or cancellation capabilities it does not
have. Choose the pool for the workload rather than expecting concurrency, autoscale, or
recycling options to create isolation.

The [pool
comparison](compare-redis.md#choose-execution-isolation-before-tuning-concurrency)
explains why solo tuning adds no execution slots. [Threads share process
state](threads-redis.md#7-gotchas-to-avoid), and backend thread-safe configuration
selects instance sharing rather than arbitrary client safety. [Gevent needs early
patching](gevent-redis.md#start-through-the-cli-with-the-pool-selected-early) and
cooperative dependencies; [prefork memory
recycling](prefork-redis.md#7-gotchas-to-avoid) occurs after a task, not at peak
allocation. GIL and native-driver behavior still matter; free-threaded Python was not
tested.

#### H8 - Redis key separation and hardening are not durability certification

*Pool: solo, prefork, threads, gevent · Type: limitation · Proofs: design limitation*

Putting the broker and results in different Redis databases does not make them
independent services. Key organization and worker settings cannot replace durability,
capacity planning, or failover testing.

The [operational
guidance](solo-redis-hardening.md#operational-hardening-that-remains-mandatory)
separates key namespaces from shared-server failure and memory pressure. Noeviction
still needs headroom and write-rejection handling, while persistence/failover has a
separate recovery-point objective. [Combined outages, production load, and
failover](solo-redis-hardening.md#deployment-acceptance-still-required) remain
acceptance tests; the broker-path fault suite kept the result backend reachable and did
not certify those scenarios.

## Recommended setups and hardening

- [Compare the pools](compare-redis.md): shared recommendations and pool differences.
  - [Solo + Redis](solo-redis.md): finite inline work, bounded dependencies, late ACK.
    - [Solo hardening](solo-redis-hardening.md): tested opt-in guards, immediate-only
      publication, and application scheduling/idempotency/reconciliation patterns.
  - [Prefork + Redis](prefork-redis.md): process isolation, limits, worker-loss policies.
  - [Threads + Redis](threads-redis.md): bounded blocking I/O and shared-state constraints.
  - [Gevent + Redis](gevent-redis.md): cooperative workloads and early patching.

These are evidence-informed starting profiles, not production certification.
Distinguish prevention, mitigation, later recovery, and future upstream fixes;
none provides an unconditional delivery or exactly-once guarantee.

## Suites and reports

| Suite directory | Report / documentation | Recorded coverage and contents |
| --- | --- | --- |
| [shutdown/](shutdown/) | [Shutdown comparison](shutdown.md), [appendix source](shutdown/shutdown-appendix.md) | 96-case four-pool matrix, native confirmation, separate two-case solo requeue check; runner/probe, renderer/tests, logs and runs |
| [eta-prefetch/](eta-prefetch/) | [ETA/prefetch](eta-prefetch.md) | 31 selected cases; probe/runner, logs and runs |
| [task-policy/](task-policy/) | [Task policies](task-policy.md) | 47 selected cases; probe/runner, logs and runs |
| [redis-connection/](redis-connection/) | [Redis connections](redis-connection.md) | 47 selected cases; wire-fault proxy/tests, probe/runner, logs and runs |
| [redis-visibility/](redis-visibility/) | [Redis visibility](redis-visibility.md) | 27 selected cases; observation/configuration tests, source/history audit, logs and runs |
| [solo-redis-hardening/](solo-redis-hardening/) | [Solo hardening](solo-redis-hardening.md) | Seven worker cases, 14 unit tests; application examples, probe/runner, logs, runs and PR snapshots |
| [environment/](environment/) | [Shared environment](environment/README.md), [portability validation](environment/portability-validation.md) | mise tasks, shared uv/tool/source locks, setup and layout checks |

Counts describe recorded suites, not every possible pool/setting combination.
Reports link to per-case evidence and separate measurements from extrapolation.
Shared environment utilities own source paths, provenance, and private Redis;
several suites also reuse shutdown observers and task-policy cleanup. Runners supply
explicit source/suite import paths, including for prefork spawn. All suites use the
same pinned sources and environment.

## Running the experiments

### Prerequisites and setup

**Run commands from this experiment repository's root.** It is self-contained:
no surrounding Celery checkout is needed. Use mise **2026.8.4** (the validated
bootstrap version) or a compatible newer version, Git, and a native build toolchain
for Redis. Inspect the configuration before trusting it:

```bash
mise trust
mise install --locked
mise run setup
```

[mise.toml](mise.toml) and [mise.lock](mise.lock) pin Python **3.13.9**, uv
**0.11.15**, and Redis **8.2.3**. [pyproject.toml](pyproject.toml) and
[uv.lock](uv.lock) define one shared Python environment for every experiment.
Setup uses `uv sync --locked`, not a fresh dependency resolution.

[repos.lock.toml](repos.lock.toml) pins the full Git revisions of Celery and Kombu.
Setup clones them into ignored `repos/celery/` and `repos/kombu/` and checks out
detached revisions. Both are imported directly through `PYTHONPATH`; neither is
installed as a Python distribution. Setup refuses to discard local source edits.
[Environment documentation](environment/README.md) explains the locks, task
commands, source checks, and deliberate upgrade procedure.

**Do not move an existing virtual environment.** When this repository is relocated,
recreate `.venv/` with `mise run setup`; virtualenv scripts contain absolute paths.
No relocation, new remote, or GitHub publication is part of this setup.

Run on an isolated development machine. The suites start real workers and private
Redis servers, own their cleanup, and deliberately inject signals, process deaths,
socket blackholes, and outages. Fixtures disable Redis persistence: they do not
validate production durability. The recorded macOS/spawn results are not Linux,
Windows, RabbitMQ, Docker, high-load, or combined-outage certification. Tool download
locks cover macOS ARM64 and Linux x64; Linux behavior has not been certified.

### Focused real-worker checks

```bash
mise run shutdown --pool solo --case warm-late
mise run solo-requeue
mise run eta-prefetch --case due-solo-late
mise run task-policy --case failure-solo-ack
mise run redis-connection --case delivery-reply-lost
mise run redis-visibility --case solo-eta-short-late
mise run solo-hardening --case cold-mixed-legacy-eta

# Focused portability controls across all suites and all four pools:
mise run smoke
```

### Full suites and case discovery

Omit `--case`/`--pool` selections to run the corresponding full suite. Normal
runners expose cases with `--help` and accept repeated `--case`; the shutdown
matrix also accepts repeated `--pool`:

```bash
mise tasks
mise run redis-connection --help
mise run shutdown
mise run solo-hardening
```

The solo-requeue task always runs its two controls unless passed `--help`;
help now exits without starting workers. Task arguments are forwarded to the
underlying suite runner. Tasks set their working directory to this repository's
root; `mise -C /path/to/experiments run TASK` also works from an unrelated directory.

### Unit and regression checks

These checks do not start real Redis/workers or install dependencies. The original
recorded suite contained **43 tests**; the current suite additionally checks the
self-contained configuration and source-setup safeguards.

```bash
mise run check
mise run test
```

All suites share `.venv/`, pinned sources, import-path setup, and the private Redis
fixture. `check` rejects version drift, wrong import origins, dirty source checkouts,
installed Celery/Kombu wheels, or a stale uv lock before worker tasks run.

### Reading artifacts and rendering the shutdown report

Runners write **new timestamped directories inside their suites**, containing
summary JSON, per-case results, logs/events, effective settings, source revisions,
and repository-relative harness hashes. They do not overwrite maintained reports or
old runs. Inspect body starts/completions, backend state, actual ACK/reject operations,
queued/unacked/restored records, and process exit separately. Runtime logs may still
contain machine-specific traceback and Unix-socket paths; these are observations,
not setup dependencies.

Only the shutdown report has a renderer:

```bash
mise run render:shutdown \
  shutdown/pool-shutdown-runs/20261113T081413.844718Z/summary.json \
  --previous shutdown/pool-shutdown-runs/20261113T080741.135372Z/summary.json
```

This command **writes `shutdown.md`**. It requires a complete 96-case summary and
includes `shutdown/shutdown-appendix.md`, rebasing relative links. Edit that appendix
source, not its rendered copy; missing/empty source prevents regeneration. Never
replace full-suite data with partial smoke results. Other reports are maintained
manually.

### Historical paths and provenance

Historical logs, run/configuration JSON, source manifests, and audit snapshots are
preserved byte-for-byte. The former pip-style dependency lock is retained as
[historical-requirements.lock](environment/historical-requirements.lock), not used
for setup. Old absolute paths and repository names describe the original runs—not
resources to reconnect to or commands to execute now. The earlier
[migration map](environment/layout-migration.json) and
[layout validation](environment/layout-validation.md) document the previous layout.
See [portability validation](environment/portability-validation.md) for this migration.
Changed runner hashes in new results are expected; historical evidence is never
rewritten to match current sources.
