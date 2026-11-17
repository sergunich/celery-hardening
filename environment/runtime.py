"""Shared repository paths, source imports, provenance and private Redis fixture."""
from contextlib import contextmanager
import hashlib
import importlib
import importlib.metadata
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]
CELERY = ROOT / "repos/celery"
KOMBU = ROOT / "repos/kombu"
SOURCE_PATHS = (CELERY, KOMBU)
SUITES = {
    "shutdown": "shutdown/verify-pool-shutdown.py",
    "solo-requeue": "shutdown/verify-solo-requeue.py",
    "eta-prefetch": "eta-prefetch/verify-eta-prefetch.py",
    "task-policy": "task-policy/verify-task-policies.py",
    "redis-connection": "redis-connection/verify-redis-connections.py",
    "redis-visibility": "redis-visibility/verify-redis-visibility.py",
    "solo-hardening": "solo-redis-hardening/verify-solo-hardening.py",
}


def load_toml(name):
    return tomllib.loads((ROOT / name).read_text())


def activate_sources():
    """Use only our source checkouts, even for CLI help from an unrelated cwd."""
    for path in reversed(SOURCE_PATHS):
        if not path.is_dir():
            raise RuntimeError(f"Missing {path.relative_to(ROOT)}; run mise run setup")
        if str(path) in sys.path:
            sys.path.remove(str(path))
        sys.path.insert(0, str(path))
    for name, path in zip(("celery", "kombu"), SOURCE_PATHS):
        module = sys.modules.get(name)
        if module is not None and not Path(module.__file__).resolve().is_relative_to(path):
            raise RuntimeError(f"{name} was already imported from outside repos/{name}")


def pythonpath(*extra):
    """Do not inherit a caller's unrelated source trees in child workers."""
    return os.pathsep.join(str(p) for p in (*SOURCE_PATHS, ROOT, *extra))


def git_output(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def source_revision(name="celery"):
    return git_output(ROOT / "repos" / name, "rev-parse", "HEAD")


def dependency_version(name):
    if name in ("celery", "kombu"):
        return importlib.import_module(name).__version__
    return importlib.metadata.version(name)


def provenance():
    """Record both source revisions and root-relative hashes of the current harness."""
    files = [ROOT / name for name in (
        "pyproject.toml", "uv.lock", "mise.toml", "mise.lock", ".python-version", "repos.lock.toml")]
    for directory in ("environment", *dict.fromkeys(Path(p).parts[0] for p in SUITES.values())):
        files.extend(sorted((ROOT / directory).glob("*.py")))
    return dict(
        source_revisions={name: source_revision(name) for name in ("celery", "kombu")},
        source_imports={name: str(Path(importlib.import_module(name).__file__).resolve().relative_to(ROOT))
                        for name in ("celery", "kombu")},
        harness_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
    )


def redis_executable():
    version = load_toml("mise.toml")["tools"]["redis"]
    install = subprocess.check_output(
        ["mise", "where", f"redis@{version}"], cwd=ROOT, text=True).strip()
    return Path(install) / "bin/redis-server"


@contextmanager
def private_redis(run_dir):
    """Own one persistence-disabled broker/backend; never touch a shared instance."""
    import redis

    socket = ROOT / f"redis-{os.getpid()}.sock"
    # AF_UNIX paths have a small platform limit; fail before starting a process.
    if len(os.fsencode(socket)) >= 104:
        raise RuntimeError("Repository path is too long for a private Redis Unix socket")
    if socket.exists():
        raise RuntimeError(f"Refusing to reuse existing socket {socket.name}")
    with (run_dir / "redis.log").open("w") as log:
        server = subprocess.Popen(
            [str(redis_executable()), "--port", "0", "--unixsocket", str(socket),
             "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", str(run_dir)],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        client = redis.Redis(unix_socket_path=str(socket))
        try:
            until = time.monotonic() + 10
            while True:
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    pass
                if server.poll() is not None or time.monotonic() > until:
                    raise RuntimeError(f"Private Redis did not start; see {run_dir.relative_to(ROOT)}/redis.log")
                time.sleep(0.05)
            yield client, f"redis+socket://{socket}?virtual_host=0", f"redis+socket://{socket}?virtual_host=1"
        finally:
            client.close()
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
            socket.unlink(missing_ok=True)
