#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
# Use the checkout through PYTHONPATH; do not install/edit the Celery package.
if [[ ! -x .dev/.venv/bin/python ]]; then
    uv venv --python 3.13 .dev/.venv
fi
if [[ ! -f .dev/environment/shutdown-requirements.lock ]]; then
    uv pip compile requirements/default.txt requirements/extras/redis.txt \
        requirements/extras/gevent.txt -o .dev/environment/shutdown-requirements.lock
fi
uv pip sync --python .dev/.venv/bin/python .dev/environment/shutdown-requirements.lock
# Isolated comparison broker; no mise project/global activation or shared Redis.
mise install redis@8.2.3
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$ROOT" .dev/.venv/bin/python -c \
    'import celery, billiard, kombu; print("Celery", celery.__version__, celery.__file__); print("billiard", billiard.__version__, "kombu", kombu.__version__)'
