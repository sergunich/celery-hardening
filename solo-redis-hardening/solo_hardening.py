"""Opt-in application examples, not Celery/Kombu patches.

Copy into your application's own package after reviewing solo-redis-hardening.md.
The cold-shutdown guard is specific to the recorded solo pool implementation.
"""
from functools import wraps

from celery import Celery
from celery.exceptions import Reject
from celery.worker import state

SOLO_KILL_JOB_ERROR = (
    "<class 'celery.concurrency.solo.TaskPool'> does not implement kill_job"
)


class TaskProcessExit(RuntimeError):
    """Task code attempted plain sys.exit/SystemExit, not worker shutdown."""


class SchedulingForbidden(ValueError):
    """This application publishes immediate Celery tasks only."""


def solo_task_guard(fun):
    """Wrap only the task body; late ACK and interruption-safe effects required."""
    @wraps(fun)
    def guarded(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except NotImplementedError as exc:
            cold_requested = (
                state.should_terminate is not None
                and state.should_terminate is not False
            )
            if not (cold_requested and str(exc) == SOLO_KILL_JOB_ERROR):
                raise
            raise Reject("Solo worker shutting down", requeue=True) from exc
        except SystemExit as exc:
            # WorkerShutdown and WorkerTerminate inherit SystemExit. Never turn
            # them (or other subclasses) into an ordinary task failure.
            if type(exc) is not SystemExit:
                raise
            raise TaskProcessExit(f"Task attempted SystemExit({exc.code!r})") from exc
    return guarded


class ImmediateCelery(Celery):
    """Reject native scheduling through this app's normal publication path.

    Not a broker-side firewall. Other apps/raw Kombu publishers can bypass it.
    """
    def send_task(self, name, args=None, kwargs=None, countdown=None, eta=None,
                  *rest, **options):
        if eta is not None or countdown not in (None, 0):
            raise SchedulingForbidden(
                "Native ETA/countdown forbidden; persist not_before in application storage"
            )
        return super().send_task(name, args, kwargs, countdown, eta, *rest, **options)
