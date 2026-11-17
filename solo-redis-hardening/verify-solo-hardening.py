#!/usr/bin/env python3
"""Small real-worker checks for application hardening, using private Redis only."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

SUITE = Path(__file__).resolve().parent
sys.path.insert(0, str(SUITE.parent))
from environment.runtime import (
    ROOT, activate_sources, provenance, pythonpath, private_redis,
)
activate_sources()
spec = importlib.util.spec_from_file_location('policy_helpers', ROOT / 'task-policy/verify-task-policies.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
helpers = policy.helpers
from solo_hardening import ImmediateCelery

CASES = [
    dict(name='warm-late', shutdown='SIGTERM'),
    dict(name='cold-late', shutdown='SIGQUIT'),
    dict(name='cold-early-negative-control', shutdown='SIGQUIT', late=False),
    dict(name='cold-mixed-legacy-eta', shutdown='SIGQUIT', future=3),
    dict(name='unrelated-error', mode='unrelated_error'),
    dict(name='guarded-system-exit', mode='system_exit'),
    dict(name='immediate-retry', mode='retry'),
]


def run_case(case, run_dir, client, broker, backend):
    folder = run_dir / case['name']
    folder.mkdir()
    client.flushall()  # Only the private server created below.
    event_path = folder / 'events.jsonl'
    config = dict(case, broker=broker, backend=backend, queue='hardening-probe',
                  events=str(event_path.relative_to(ROOT)), pool='solo')
    (folder / 'config.json').write_text(json.dumps(config, indent=2) + '\n')
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1',
               PYTHONPATH=pythonpath(SUITE, ROOT / 'shutdown'),
               SHUTDOWN_PROBE_CONFIG=json.dumps(config))
    for key in ('REMAP_SIGTERM', 'CELERY_RDBSIG'):
        env.pop(key, None)
    command = [sys.executable, '-m', 'celery', '-A', 'hardening_probe:app', 'worker',
               '--pool=solo', '--concurrency=1', '--queues=hardening-probe',
               '--hostname=hardening-probe@localhost', '--loglevel=INFO',
               '--without-gossip', '--without-mingle', '--without-heartbeat']
    publisher = ImmediateCelery('hardening_publisher', broker=broker, backend=backend,
                                set_as_current=False)
    # Explicit bypass models waiting messages from before the policy was deployed.
    legacy = helpers.Celery('legacy_publisher', broker=broker, backend=backend,
                            set_as_current=False)
    task_id = str(uuid.uuid4())
    futures, errors, actions = [], [], []
    forced = False

    def events():
        return helpers.read_events(event_path)

    with (folder / 'worker.log').open('w') as log:
        worker = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                  stderr=subprocess.STDOUT, start_new_session=True)
        def wait_for(predicate, description, timeout=12):
            end = time.monotonic() + timeout
            while not predicate():
                if worker.poll() is not None or time.monotonic() >= end:
                    raise RuntimeError(f'{description}; worker exit={worker.poll()}')
                time.sleep(0.02)
        try:
            wait_for(lambda: any(e['event'] == 'worker_ready' for e in events()), 'worker ready')
            for _ in range(case.get('future', 0)):
                future_id = str(uuid.uuid4())
                legacy.send_task('hardening_probe.work', args=[0.01], task_id=future_id,
                                 countdown=60, queue='hardening-probe')
                futures.append(future_id)
            if futures:
                wait_for(lambda: all(any(e['event'] == 'hardening_received' and
                                         e.get('task_id') == task for e in events())
                                     for task in futures), 'future deliveries received')
            duration = 3 if case.get('shutdown') == 'SIGQUIT' else 0.3
            publisher.send_task('hardening_probe.work', args=[duration, case.get('mode', 'sleep')],
                                task_id=task_id, queue='hardening-probe')
            wait_for(lambda: any(e['event'] == 'hardening_started' and
                                 e.get('task_id') == task_id for e in events()), 'body started')
            if not case.get('shutdown'):
                wait_for(lambda: publisher.backend.get_task_meta(task_id)['status'] in
                         ('SUCCESS', 'FAILURE'), 'terminal task result')
                publisher.send_task('hardening_probe.ping', queue='hardening-probe')
                wait_for(lambda: any(e['event'] == 'hardening_ping' for e in events()), 'credit usable')
            else:
                time.sleep(0.08)
            sig = case.get('shutdown', 'SIGTERM')
            actions.append(dict(signal=sig, time=time.monotonic()))
            worker.send_signal(getattr(signal, sig))
            worker.wait(timeout=12)
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            if worker.poll() is None:
                forced = True
                helpers.kill_group(worker.pid)
                worker.wait(timeout=10)
            policy.cleanup_group(worker)

    observed = events()
    active = [e for e in observed if e.get('task_id') == task_id]
    body_count = sum(e['event'] == 'hardening_completed' for e in active)
    starts = sum(e['event'] == 'hardening_started' for e in active)
    acks = sum(e['event'] == 'message_ack' for e in active)
    rejects = [e.get('requeue') for e in active if e['event'] == 'message_reject']
    result = publisher.backend.get_task_meta(task_id)
    snapshot = helpers.broker_snapshot(client, task_id)
    future_snapshots = {task: helpers.broker_snapshot(client, task) for task in futures}
    cold = case.get('shutdown') == 'SIGQUIT'
    late = case.get('late', True)
    failed = case.get('mode') in ('system_exit', 'unrelated_error')
    expected_state = 'PENDING' if cold else 'FAILURE' if failed else 'SUCCESS'
    expected_acks = 0 if cold and late else 2 if case.get('mode') == 'retry' else 1
    checks = {
        'no safety kill': not forced,
        'clean worker exit': worker.returncode == 0,
        'body starts': starts == (2 if case.get('mode') == 'retry' else 1),
        'body completion': body_count == (0 if cold or failed else 1),
        'backend state': result['status'] == expected_state,
        'ACK count': acks == expected_acks,
        'reject policy': rejects == ([True] if cold and late else []),
        'active delivery settlement': len(snapshot['queued']) == int(cold and late)
                                      and not snapshot['unacked'],
        'waiting messages restored': all(len(s['queued']) == 1 and not s['unacked']
                                        for s in future_snapshots.values()),
        'waiting messages untouched': not any(e.get('task_id') in futures and e['event'] in
            ('hardening_started', 'message_ack', 'message_reject') for e in observed),
    }
    if case.get('mode') == 'system_exit':
        checks['SystemExit converted'] = type(result['result']).__name__ == 'TaskProcessExit'
    if case.get('mode') == 'unrelated_error':
        checks['ordinary exception preserved'] = isinstance(result['result'], NotImplementedError)
    errors.extend(name for name, passed in checks.items() if not passed)
    outcome = dict(case=case, passed=not errors, errors=errors, task_id=task_id, future_ids=futures,
                   starts=starts, completed=body_count, acks=acks, rejects=rejects,
                   state=result['status'], broker=snapshot, future_broker=future_snapshots,
                   returncode=worker.returncode, forced_kill=forced, actions=actions)
    (folder / 'result.json').write_text(json.dumps(outcome, indent=2) + '\n')
    publisher.close()
    legacy.close()
    print(f"{'PASS' if outcome['passed'] else 'FAIL'} {case['name']}: "
          f"start/done/ACK={starts}/{body_count}/{acks} reject={rejects} "
          f"state={result['status']} exit={worker.returncode} errors={errors}", flush=True)
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', action='append', choices=[c['name'] for c in CASES])
    args = parser.parse_args()
    run_dir = SUITE / 'runs' / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    run_dir.mkdir(parents=True)
    sources = [SUITE / name for name in ('solo_hardening.py', 'hardening_probe.py',
                                         'verify-solo-hardening.py', 'test-solo-hardening.py')]
    summary = dict(provenance=provenance(), source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in sources}, outcomes=[])
    with helpers.private_redis(run_dir) as (client, broker, backend):
        summary['redis_server'] = client.info('server')['redis_version']
        for case in CASES:
            if args.case and case['name'] not in args.case:
                continue
            summary['outcomes'].append(run_case(case, run_dir, client, broker, backend))
            (run_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(f'Artifacts: {run_dir.relative_to(ROOT)}')
    return 0 if all(o['passed'] for o in summary['outcomes']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
