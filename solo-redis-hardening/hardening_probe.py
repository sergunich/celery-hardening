"""Real-worker probe for opt-in application wrappers; not deployment code."""
import time

from celery import signals
from celery.exceptions import Reject

# Reuse observation-only ACK/reject/signal hooks and JSON event writer. The
# original shutdown app is not the worker app exported below.
from shutdown_probe import CONFIG, emit
from solo_hardening import ImmediateCelery, SchedulingForbidden, TaskProcessExit, solo_task_guard

app = ImmediateCelery('hardening_probe', broker=CONFIG['broker'], backend=CONFIG['backend'])
app.conf.update(
    task_default_queue='hardening-probe',
    task_acks_late=CONFIG.get('late', True),
    task_acks_on_failure_or_timeout=True,
    task_ignore_result=False,
    worker_deduplicate_successful_tasks=True,
    worker_prefetch_multiplier=1,
    worker_eta_task_limit=8,
    worker_soft_shutdown_timeout=0,
    worker_cancel_long_running_tasks_on_connection_loss=False,
    worker_redirect_stdouts=False,
    broker_transport_options={'visibility_timeout': 3600},
)


@app.task(bind=True, name='hardening_probe.work', max_retries=1, default_retry_delay=0,
          dont_autoretry_for=(Reject, TaskProcessExit, SchedulingForbidden))
@solo_task_guard
def work(self, duration=0.2, mode='sleep'):
    emit('hardening_started', task_id=self.request.id, retries=self.request.retries)
    try:
        if mode == 'system_exit':
            raise SystemExit(23)
        if mode == 'unrelated_error':
            raise NotImplementedError('ordinary application error')
        if mode == 'retry' and self.request.retries == 0:
            raise self.retry(exc=RuntimeError('transient probe error'), countdown=0)
        time.sleep(duration)
        emit('hardening_completed', task_id=self.request.id)
        return 'completed'
    finally:
        emit('hardening_finally', task_id=self.request.id)


@app.task(name='hardening_probe.ping')
def ping():
    emit('hardening_ping')
    return 'alive'


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    emit('hardening_received', task_id=request.id,
         eta=request.eta.isoformat() if request.eta else None)
