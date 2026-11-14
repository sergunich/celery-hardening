## Appendix: shutdown, acknowledgment, and signal semantics

> **Maintained explanation, not generated experiment output.** Edit
> `.dev/shutdown-appendix.md`, not this appendix's rendered copy in
> `.dev/shutdown-results.md`. The report generator includes this file unchanged
> on every run and refuses to overwrite the report if it is missing or empty.
> These explanations describe the tested checkout; review them explicitly if
> a future version changes the implementation.

### A1. Does warm shutdown let a running task continue indefinitely?

**Yes: warm shutdown itself imposes no deadline.** The task can continue until
it returns, raises an exception, or is stopped by something else: a supported
pool/task time limit, an application timeout, another shutdown signal, or the
supervisor.

#### On the first warm-shutdown signal

With the default signal mapping:

- `SIGTERM` requests warm shutdown.
- The first Ctrl+C (`SIGINT`) also requests warm shutdown.

The signal handler sets Celery's `should_stop` flag and returns. **It does not
raise a cancellation exception inside the task.**

When the worker notices the flag, it starts shutdown: stops consumption, lets
running work finish, stops worker components, and closes broker connections.
Stopping consumption is not an instantaneous guarantee that no already-delivered
or already-submitted work can start; pool scheduling and shutdown races matter.

With **solo**, the consumer loop cannot notice the flag while it is executing
the task inline. The signal handler returns to the task, and shutdown proceeds
after the task returns.

#### After the task finishes

Celery processes the normal task outcome: stores the result, handles
ACK/reject/retry according to policy, and completes worker teardown.

A task calling `self.retry()` publishes another attempt. Warm shutdown does not
mean this worker must stay alive to execute that future attempt.

Additional `SIGTERM` signals do **not** escalate warm shutdown into cold shutdown.

#### What happens to prefetched tasks?

**Fetched, accepted for execution, and ACKed are different stages.**

| Task location/state | Expected shutdown handling |
| --- | --- |
| Still in the broker queue | Remains available to other workers |
| Reserved locally but not accepted for execution | Normally remains unacknowledged and is restored/redelivered during connection cleanup |
| Already accepted/running | Allowed to finish during warm shutdown |
| Already ACKed | The broker cannot automatically recover it just because the worker exits |

There are pool-specific details and races. This checkout cancels pending
thread-pool futures during warm shutdown; prefork also has logic to discard
work not yet accepted by children. Do not interpret this as a guarantee that
all submitted-but-not-yet-running work behaves identically across pools.

**The experiments published one task per case and did not validate a prefetched
backlog.** This explanation comes from the shutdown implementation, not a
runtime backlog test. Broker-specific recovery and forced termination can also
change the outcome.

### A2. What is `worker_soft_shutdown_timeout` for?

It adds a grace period **when cold shutdown is requested**, rather than placing
a deadline on warm shutdown.

```text
TERM
  → warm shutdown
  → wait for running tasks without a shutdown-imposed deadline

QUIT
  → soft grace period, if enabled
  → cold cancellation of remaining tasks
  → teardown
```

For example:

```python
worker_soft_shutdown_timeout = 10
```

When `SIGQUIT` is sent:

1. Celery enters the cold-shutdown path.
2. It gives executing tasks a 10-second completion window.
3. It then attempts to cancel whatever remains.

That window starts when **cold shutdown is requested**, not when an earlier
TERM arrived. It is a grace period before attempted cancellation, not an exact
process-exit deadline; teardown can take additional time.

The cold path can be entered through:

- `SIGQUIT`.
- Ctrl+C escalation.
- `SIGTERM` if explicitly configured with `REMAP_SIGTERM=SIGQUIT`.

**Setting the timeout alone does not activate a timer after ordinary TERM.**

Use it when cold shutdown should give short remaining work a chance to finish
before cancellation. Prefork demonstrated that behavior in these tests. Threads
could finish during the window but could not forcibly cancel overrunning work;
solo and gevent had the limitations documented in the comparison.

When idle, the wait is normally skipped.
`worker_enable_soft_shutdown_on_idle=True` enables it even without active tasks.

### A3. Why ACK a failed task? Should solo catch the error and retry?

#### ACK does not mean “the task succeeded”

It means approximately: **“This delivery has been handled; the broker should
remove it.”**

Celery normally treats an application exception as a completed, failed attempt,
not as a reason for unlimited broker redelivery. With late acknowledgments, the
default acknowledge-on-failure policy therefore ACKs failed tasks too.

The solo cold-shutdown case followed this path:

```text
Cancellation calls unsupported terminate_job()
  → NotImplementedError enters task execution
  → Celery records ordinary task FAILURE
  → acknowledge-on-failure policy ACKs the delivery
```

**Turning off acknowledge-on-failure does not automatically fix this.** In this
checkout, an ordinary failed task is then rejected **without requeue**, rather
than automatically retried. See `task_acks_on_failure` and its fallback setting
`task_acks_on_failure_or_timeout`; this policy is relevant to late-ack tasks.

#### Recommended solo shutdown strategy

**Prefer TERM and allow the task to finish.** Give the supervisor enough grace
time, and use application-level I/O timeouts/checkpoints so tasks do not hang
indefinitely.

Do **not** treat any `NotImplementedError` as “Celery asked me to shut down”:

- It is an implementation failure, not a stable cancellation API.
- The same exception could represent a genuine application bug.
- Interruption can occur after side effects have already happened.
- Retrying can duplicate those effects.
- Publishing a replacement attempt is not atomic with acknowledging the
  original delivery.

If cooperative interruption is required, design it explicitly: check a stop
condition at safe checkpoints, preserve progress, and use `self.retry()` for
the intended retry policy. That is application-level behavior, not a workaround
based on catching Celery's unsupported cancellation error.

For independently terminating task execution, prefork is the better fit, subject
to its own signal, native-code, and delivery limitations.

### A4. Why was the solo “early-ack” task unacked after SIGKILL?

Two distinctions matter:

1. **Early ACK is normally requested when the execution pool accepts the task**,
   not simply when the broker delivers it into the worker's prefetch buffer.
2. **Requesting an ACK does not necessarily execute the broker operation immediately.**

In this checkout's Redis async-consumer path:

```text
Broker delivers message
  ↓
Solo accepts task for execution
  ↓
Request.acknowledge()
  ↓
Schedule broker ACK callback on the event loop
  ↓
Run task inline in the same thread
```

Solo then occupies the event-loop thread. If SIGKILL arrives before that thread
can process the queued callback, **Redis never receives the ACK operation**.

This explains the experiment. It does not make early acknowledgment a dependable
recovery strategy: different pools, transports, or execution timing can send the
ACK earlier.

### A5. How are ACK callbacks queued, and how should this work?

The relevant chain is:

```text
Request.acknowledge()
  → consumer.call_soon_ack(message.ack_log_error, ...)
    → hub.call_soon(callback)
```

`hub.call_soon()` adds the callback to the hub's ready-callback collection.
When the event loop gets control, it removes and executes ready callbacks.
For Redis, the eventual ACK removes the delivery from its unacked bookkeeping.

This scheduling keeps broker operations on the consumer/event-loop execution
path, including when pool callbacks originate elsewhere. The chain above
specifically describes the **hub-enabled path** used by the solo Redis test;
this checkout can execute ACK callbacks directly when no hub is present.

With prefork or threads:

```text
Controller/event loop: schedule ACK → execute ACK → continue handling events
Task execution:                         runs independently
```

With solo:

```text
One thread: schedule ACK → execute entire task → return control → process ACK
```

Queued ACKs can also be processed during shutdown cleanup. The task returning
does not guarantee that an ordinary next loop iteration is what executes them,
nor that abrupt termination will allow the callbacks to run.

An important implementation detail: `Request.acknowledged` is set after invoking
the scheduling callback. Therefore it can already be `True` **while the actual
broker operation is still pending**. That is why the tests observe ACK operations
and Redis state separately.

Relevant source: `celery/worker/request.py` (`on_accepted`, `acknowledge`),
`celery/worker/consumer/consumer.py` (`create_task_handler`, `call_soon_ack`),
Kombu's `asynchronous/hub.py` (`call_soon`, `create_loop`), and its Redis transport.

### A6. What does “SIGINTs coalesced while Python was in C” mean?

**Signals are not a reliable queue of button presses.**

There are two relevant layers:

- Standard OS signals such as SIGINT can merge while pending.
- CPython normally first handles a signal through a small low-level handler
  that marks it pending. The Python-level handler runs later, when the main
  thread can execute it. Repeated occurrences of the same signal need not be
  counted separately.

The native-code experiment effectively did this:

```text
0.0 s   Main thread enters PBKDF2 in C
0.3 s   SIGINT: mark SIGINT pending
0.6 s   SIGINT: SIGINT is still pending
0.9 s   SIGINT: SIGINT is still pending
1.2 s   SIGINT: SIGINT is still pending
~2 s    C call returns; Python runs one pending SIGINT handler
```

That handler was still Celery's **first Ctrl+C handler**, so it requested warm
shutdown and installed the handler for the next stage. It did **not** receive
a preserved count of four presses and jump through four stages.

This particularly affects solo/gevent when native work occupies their shared
main thread. In prefork, the parent can handle signals while its child executes
native code. In the threads test, the native function released the GIL, allowing
the controller to run. Other native functions can behave differently; releasing
the GIL does not itself make the interrupted main thread execute Python handlers.

This is not a claim that every burst of signals collapses, or that different
signal types merge into one. The outcome depends on which signals arrive and
when handlers get a chance to run.

**Practical consequence:** do not implement an external shutdown deadline by
rapidly sending several Ctrl+C signals. Use explicit shutdown stages and an
independently enforced final SIGKILL deadline.

### A7. Follow-up: ETA/countdown, prefetch, and delivery limits

The [focused ETA/prefetch report](eta-prefetch-results.md) adds **31 selected
runtime cases**, including the prefetched-backlog coverage absent from the
original 96-case shutdown comparison. It separates measured results from
source-based extrapolations and explains why the reduced matrix was chosen.

Key findings: waiting ETA tasks occupy memory/timer entries, not execution slots,
and remain unacked even with early ACK configured. Ordinary prefetch expands
as ETA tasks are scheduled; `worker_eta_task_limit` is the intended bound, but
its interaction with zero/oversized initial prefetch and subsequent decrements
has limitations in the tested Kombu version. Waiting ETA messages and running
tasks can have different ACK/recovery outcomes during the same shutdown.

That follow-up is separately maintained; experiment runs do not overwrite its
explanations or this appendix.

### A8. Requeueing a running solo task after the QUIT `kill_job` error

**There is no configuration-only guarantee.** For the specific unsupported
cancellation failure in this checkout, a task-level workaround is:

1. Enable **late acknowledgment**.
2. Catch the shutdown-induced exception inside the task.
3. Raise **`Reject(requeue=True)`**, rather than letting Celery classify the
   exception as an ordinary task failure.

#### Runtime verification

The same Redis-backed harness was used for two focused cases. QUIT was sent
while the task was executing inside the guarded work block.

| Configuration | Task body finished? | Backend result | ACK sent? | Reject sent? | Original message after exit |
| --- | --- | --- | --- | --- | --- |
| Late ACK + `Reject(requeue=True)` | No | PENDING | No | Yes, requeue enabled | **Restored; fetched again with the original task ID** |
| Early ACK + `Reject(requeue=True)` | No | PENDING | Yes | No | Absent; not restored |

Both workers exited with code 0 without a safety-deadline kill. Both targeted
checks passed. The late-ack outcome demonstrates successful requeueing of the
original delivery, not successful completion of its work or an exactly-once
execution guarantee.

#### Version-specific workaround

```python
from celery.exceptions import Reject
from celery.worker import state


@app.task(acks_late=True)
def my_task(payload):
    try:
        return do_work(payload)
    except NotImplementedError as exc:
        cold_requested = (
            state.should_terminate is not None
            and state.should_terminate is not False
        )
        unsupported_cancellation = (
            "does not implement kill_job" in str(exc)
        )

        if not (cold_requested and unsupported_cancellation):
            raise

        raise Reject(
            "Solo worker shutting down",
            requeue=True,
        ) from exc
```

This changes the outcome from:

```text
NotImplementedError → ordinary FAILURE → ACK
```

to:

```text
NotImplementedError → Reject(requeue=True) → original delivery requeued
```

Unlike `self.retry()`, this requests requeueing of the **original message**,
rather than publishing another attempt. It is intentionally narrow: do not
turn unrelated `NotImplementedError` exceptions into shutdown retries.

#### Why late acknowledgment is required

`Request.reject()` does nothing once `Request.acknowledged` is true. With early
ACK, that flag can already be true even while the actual Redis ACK is still
queued. In the early-ack control test, rejection was ignored and the queued ACK
subsequently executed. An already acknowledged message cannot be recovered by
rejecting it later.

These configuration settings alone are not substitutes:

- `task_acks_on_failure=False`: ordinary failures are rejected **without requeue**
  in this checkout.
- `task_reject_on_worker_lost=True`: the unsupported-cancellation exception is
  not a `WorkerLostError`.

#### Limits and recommendation

**This is a workaround for the current implementation, not a stable shutdown
API.** It relies on private worker state and the exception text. The tests only
establish the path where the exception reaches the guarded task code.

It cannot guarantee recovery when:

- The signal interrupts code outside the `try` block.
- Inner application code catches or transforms the exception first.
- Broker communication or shutdown cleanup fails.
- Further signals interrupt rejection processing.

Requeueing **does not undo completed side effects**. The task must tolerate
execution again. If an abrupt exit leaves the delivery unacked rather than
explicitly requeued, Redis recovery may depend on visibility-timeout processing;
that is not immediate restoration.

Prefer **TERM for solo**. If QUIT-based cancellation and recovery are operational
requirements, use prefork or fix the worker cancellation path rather than relying
permanently on this task-level workaround.

#### Reproduce and inspect

```bash
.dev/.venv/bin/python .dev/verify-solo-requeue.py
```

The experiment-only `catch="requeue"` branch in `.dev/shutdown_probe.py` handles
the recognized error and raises `Reject`; no Celery production code is patched.
The runner checks actual ACK/reject calls, final Redis message state, and whether
the restored original task ID can be fetched.

Artifacts: [two-case summary](solo-requeue-runs/20261114T090428.875791Z/summary.json),
with per-case worker logs and event timelines under the same run directory.
This explanation is part of the maintained appendix source and survives future
shutdown-report regeneration.

### A9. Follow-up: task time limits, failure ACKs, and worker-loss policies

The [task-policy report](task-policy-results.md) adds **47 selected runtime
cases**, distinguishing pool deadline enforcement, failure disposition, and
whether a surviving controller can detect executor loss.

For solo, ordinary failure ACK policy works, but neither soft nor hard task
limits are enforced. A dead solo process cannot run worker-loss recovery.
The QUIT `kill_job` error is an ordinary failure: disabling failure ACK changes
ACK to discard, not requeue, even with both worker-loss flags enabled.

The report also covers bounded prefork worker-loss retries, soft exceptions
using failure rather than timeout ACK policy, a non-prefork `SystemExit`
callback defect, and gevent requeueing after a non-yielding task body has already
completed. It explicitly separates synthetic exception tests from actual
process-loss recovery, with measured ACK/reject/result/redelivery outcomes.
