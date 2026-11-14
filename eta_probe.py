"""ETA/prefetch probe, reusing the shutdown suite's real app and ACK observer."""
import time

from celery import signals
from celery.worker import state
from celery.worker.request import Request

from shutdown_probe import CONFIG, app, emit

app.conf.update(
    worker_prefetch_multiplier=CONFIG.get("prefetch", 1),
    worker_eta_task_limit=CONFIG.get("eta_limit"),
    worker_disable_prefetch=CONFIG.get("disable_prefetch", False),
)


class ObservedRequest(Request):
    def on_accepted(self, pid, time_accepted):
        super().on_accepted(pid, time_accepted)
        emit("request_accepted", task_id=self.id, ack_requested=self.acknowledged)


@app.task(bind=True, name="eta_probe.work", Request=ObservedRequest)
def work(self, role, duration=0.2):
    emit("eta_started", task_id=self.request.id, role=role, eta=self.request.eta)
    try:
        steps = max(1, round(duration / 0.1))
        for step in range(steps):
            time.sleep(duration / steps)
            emit("eta_tick", task_id=self.request.id, role=role, step=step)
        emit("eta_completed", task_id=self.request.id, role=role)
        return role
    except BaseException as exc:
        emit("eta_interrupted", task_id=self.request.id, exception=type(exc).__name__)
        raise
    finally:
        emit("eta_finally", task_id=self.request.id, role=role)


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    emit("eta_received", task_id=request.id,
         eta=request.eta.isoformat() if request.eta else None,
         ack_requested=request.acknowledged)


@signals.worker_ready.connect(weak=False)
def observe_state(sender=None, **kwargs):
    consumer = sender

    def snapshot():
        emit("scheduler_state", scheduled=sorted(r.id for r in state.scheduled_requests),
             reserved=sorted(r.id for r in state.reserved_requests),
             active=sorted(r.id for r in state.active_requests),
             qos_value=consumer.qos.value, qos_applied=consumer.qos.prev,
             channel_prefetch=consumer.task_consumer.channel.qos.prefetch_count
             if consumer.task_consumer else None)

    consumer.timer.call_repeatedly(0.1, snapshot)
    snapshot()
