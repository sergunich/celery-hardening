"""Real-worker visibility probes. Wrappers observe, then delegate unchanged."""
import json
import os
import time

from celery import Celery, signals
from celery.worker.consumer.tasks import Tasks
from celery.worker.request import Request
from kombu.message import Message
from kombu.transport.redis import QoS

CONFIG = json.loads(os.environ["VISIBILITY_PROBE_CONFIG"])
FD = os.open(CONFIG["events"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)


def emit(event, **fields):
    record = dict(event=event, time=time.monotonic(), wall=time.time(), pid=os.getpid(),
                  worker=CONFIG["label"], **fields)
    os.write(FD, (json.dumps(record) + "\n").encode())


app = Celery("visibility_probe", broker=CONFIG["broker"], backend=CONFIG["backend"])
options = dict(health_check_interval=1, socket_timeout=1, socket_connect_timeout=0.5)
if not CONFIG.get("default_scan"):
    options.update(unacked_restore_interval=CONFIG.get("scan_interval", 0.25),
                   unacked_restore_throttle=CONFIG.get("scan_throttle", 1))
if "B" in CONFIG:
    options["visibility_timeout"] = CONFIG["B"]
if CONFIG.get("prefix"):
    options["global_keyprefix"] = CONFIG["prefix"]
app.conf.update(
    broker_transport_options=options,
    result_backend_transport_options=({"visibility_timeout": CONFIG["R"]} if "R" in CONFIG else {}),
    task_default_queue=CONFIG["queue"],
    task_acks_late=CONFIG.get("acks_late", True),
    task_acks_on_failure_or_timeout=True,
    worker_deduplicate_successful_tasks=CONFIG.get("dedup", False),
    worker_prefetch_multiplier=CONFIG.get("prefetch", 2),
    worker_enable_remote_control=False,
    worker_redirect_stdouts=False,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=3,
    result_expires=CONFIG.get("result_expires", 86400),
)
if "T" in CONFIG:
    app.conf.visibility_timeout = CONFIG["T"]

_original_append = QoS.append
_original_restore = QoS.restore_by_tag
_original_scan = QoS.restore_visible
_original_ack = Message.ack
_original_reject = Message.reject
_original_tasks_start = Tasks.start


def append(self, message, tag):
    result = _original_append(self, message, tag)
    emit("registered", task_id=message.headers.get("id"), tag=tag,
         visibility=self.visibility_timeout)
    return result


def restore(self, tag, *args, **kwargs):
    observed_tag = tag.decode() if isinstance(tag, bytes) else tag
    emit("restore_attempt", tag=observed_tag, visibility=self.visibility_timeout,
         scan=bool(getattr(self, "_probe_scanning", False)))
    result = _original_restore(self, tag, *args, **kwargs)
    emit("restore_returned", tag=observed_tag)
    return result


def scan(self, *args, **kwargs):
    interval = kwargs.get("interval", args[2] if len(args) > 2 else 10)
    actual = self._vrestore_count % (interval or 1) == 0
    emit("scan_call", visibility=self.visibility_timeout, actual=actual,
         count=self._vrestore_count, throttle=interval)
    previous = getattr(self, "_probe_scanning", False)
    self._probe_scanning = True
    try:
        return _original_scan(self, *args, **kwargs)
    finally:
        self._probe_scanning = previous


def ack(self, *args, **kwargs):
    try:
        result = _original_ack(self, *args, **kwargs)
    except BaseException as exc:
        emit("ack_error", task_id=self.headers.get("id"), exception=type(exc).__name__)
        raise
    emit("ack", task_id=self.headers.get("id"), tag=self.delivery_tag)
    return result


def reject(self, *args, **kwargs):
    result = _original_reject(self, *args, **kwargs)
    emit("reject", task_id=self.headers.get("id"),
         requeue=kwargs.get("requeue", args[0] if args else False))
    return result


def tasks_start(self, consumer):
    result = _original_tasks_start(self, consumer)
    channel = consumer.task_consumer.channel
    emit("resolved", channel_visibility=channel.visibility_timeout,
         qos_visibility=channel.qos.visibility_timeout,
         broker_options=app.conf.broker_transport_options,
         backend_options=app.conf.result_backend_transport_options,
         top_present="visibility_timeout" in app.conf,
         top_value=app.conf.get("visibility_timeout"),
         backend_connection_visibility=app.backend.connparams.get("visibility_timeout"),
         result_expires=app.backend.expires, backend_persistent=app.backend.persistent,
         unacked_key=channel.qos.unacked_key, unacked_index_key=channel.qos.unacked_index_key,
         prefix=channel.global_keyprefix, scan_interval=channel.unacked_restore_interval,
         scan_throttle=channel.unacked_restore_throttle,
         qos_global=consumer.qos_global)
    return result


QoS.append = append
QoS.restore_by_tag = restore
QoS.restore_visible = scan
Message.ack = ack
Message.reject = reject
Tasks.start = tasks_start


class ObservedRequest(Request):
    def on_accepted(self, pid, time_accepted):
        super().on_accepted(pid, time_accepted)
        emit("accepted", task_id=self.id, acknowledged=self.acknowledged)


@app.task(bind=True, name="redis_visibility_probe.work", Request=ObservedRequest)
def work(self, role, duration):
    attempt = self.backend.client.incr(f"visibility-attempt:{self.request.id}")
    emit("started", task_id=self.request.id, role=role, attempt=attempt,
         retries=self.request.retries, redelivered=self.request.delivery_info.get("redelivered", False))
    time.sleep(duration if attempt == 1 else 0.12)
    emit("completed", task_id=self.request.id, role=role, attempt=attempt)
    return role


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    emit("received", task_id=request.id, tag=request._message.delivery_tag,
         redelivered=request.delivery_info.get("redelivered", False),
         eta=request.eta.isoformat() if request.eta else None)


@signals.worker_ready.connect(weak=False)
def ready(**kwargs):
    emit("ready")


@signals.after_task_publish.connect(weak=False)
def published(headers=None, **kwargs):
    emit("task_published", task_id=headers.get("id"), retries=headers.get("retries"))
