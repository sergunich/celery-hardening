"""Real tasks + observation-only hooks for Redis connection experiments."""
import time

from celery import signals
from celery.worker import state
from celery.worker.consumer.consumer import Consumer
from celery.worker.consumer.tasks import Tasks
from celery.worker.request import Request
from kombu.message import Message

from shutdown_probe import CONFIG, app, emit

app.conf.update(
    task_acks_late=CONFIG.get("acks_late", True),
    worker_cancel_long_running_tasks_on_connection_loss=CONFIG.get("cancel", False),
    worker_deduplicate_successful_tasks=CONFIG.get("dedup", False),
    worker_enable_prefetch_count_reduction=CONFIG.get("reduce_prefetch", True),
    worker_prefetch_multiplier=CONFIG.get("prefetch", 2),
    worker_enable_remote_control=CONFIG.get("control", True),
    worker_send_task_events=False,
    broker_connection_retry=CONFIG.get("retry", True),
    broker_connection_retry_on_startup=CONFIG.get("startup_retry", True),
    broker_connection_max_retries=CONFIG.get("max_retries", 4),
    broker_heartbeat=CONFIG.get("broker_heartbeat", 2),
    broker_transport_options={
        "visibility_timeout": CONFIG.get("visibility", 3600),
        "unacked_restore_interval": 0.5,
        "unacked_restore_throttle": 1,
        "health_check_interval": CONFIG.get("health", 1),
        "brpop_timeout": 1,
        "socket_connect_timeout": 0.5,
        "socket_timeout": CONFIG.get("socket_timeout", 1),
        "socket_keepalive": False,
        "retry_on_timeout": False,
    },
)

_original_connect = Consumer.connect
_original_error = Consumer.on_connection_error_after_connected
_original_tasks_start = Tasks.start
_original_ack = Message.ack


def observe_connect(self):
    emit("net_connect_attempt", generation=self.restart_count)
    conn = _original_connect(self)
    emit("net_connected", generation=self.restart_count, supports_heartbeats=conn.supports_heartbeats,
         negotiated_heartbeat=conn.get_heartbeat_interval(), configured_heartbeat=self.amqheartbeat,
         eventloop=bool(self.hub))
    return conn


def observe_error(self, exc):
    emit("net_connection_error", exception=type(exc).__name__, message=str(exc),
         active=[dict(task_id=r.id, acknowledged=r.acknowledged) for r in state.active_requests])
    result = _original_error(self, exc)
    emit("net_error_handled", initial_prefetch=self.initial_prefetch_count,
         active=[r.id for r in state.active_requests])
    return result


def observe_tasks_start(self, consumer):
    result = _original_tasks_start(self, consumer)
    emit("net_consumer_started", generation=consumer.restart_count,
         prefetch=consumer.initial_prefetch_count, qos_global=consumer.qos_global)
    return result


def observe_ack(self, *args, **kwargs):
    try:
        return _original_ack(self, *args, **kwargs)
    except BaseException as exc:
        emit("net_ack_error", task_id=self.headers.get("id"), tag=self.delivery_tag,
             exception=type(exc).__name__, message=str(exc))
        raise


Consumer.connect = observe_connect
Consumer.on_connection_error_after_connected = observe_error
Tasks.start = observe_tasks_start
Message.ack = observe_ack


class ObservedRequest(Request):
    def on_accepted(self, pid, time_accepted):
        super().on_accepted(pid, time_accepted)
        emit("net_accepted", task_id=self.id, tag=self._message.delivery_tag,
             executor_pid=pid, ack_requested=self.acknowledged)

    def cancel(self, *args, **kwargs):
        emit("net_cancel_attempt", task_id=self.id)
        try:
            result = super().cancel(*args, **kwargs)
        except BaseException as exc:
            emit("net_cancel_error", task_id=self.id, exception=type(exc).__name__)
            raise
        emit("net_cancel_returned", task_id=self.id)
        return result


@app.task(bind=True, name="redis_connection_probe.work", Request=ObservedRequest)
def work(self, role, duration=0.15, busy=False):
    attempt = self.backend.client.incr(f"net-attempt:{self.request.id}")
    emit("net_started", task_id=self.request.id, role=role, attempt=attempt,
         redelivered=self.request.delivery_info.get("redelivered", False), retries=self.request.retries)
    # Bound duplicates without rewriting retries/redelivery metadata.
    actual_duration = duration if attempt == 1 else 0.15
    try:
        end = time.monotonic() + actual_duration
        while time.monotonic() < end:
            if not busy:
                time.sleep(0.03)
        emit("net_completed", task_id=self.request.id, role=role, attempt=attempt)
        return role
    except BaseException as exc:
        emit("net_interrupted", task_id=self.request.id, role=role, exception=type(exc).__name__)
        raise
    finally:
        emit("net_finally", task_id=self.request.id, role=role, attempt=attempt)


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    emit("net_received", task_id=request.id, tag=request._message.delivery_tag,
         retries=request.request_dict.get("retries"), redelivered=request.delivery_info.get("redelivered", False),
         eta=request.eta.isoformat() if request.eta else None)


@signals.after_task_publish.connect(weak=False)
def published(headers=None, **kwargs):
    emit("net_task_published", task_id=headers.get("id"), retries=headers.get("retries"))


@signals.worker_ready.connect(weak=False)
def ready(sender=None, **kwargs):
    consumer = sender

    def snapshot():
        emit("net_state", active=[r.id for r in state.active_requests],
             scheduled=[r.id for r in state.scheduled_requests],
             reserved=[r.id for r in state.reserved_requests],
             qos=consumer.qos.value if consumer.qos else None,
             prefetch_restored=consumer._maximum_prefetch_restored)

    consumer.timer.call_repeatedly(0.2, snapshot)
    snapshot()
