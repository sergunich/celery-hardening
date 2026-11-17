#!/usr/bin/env python3
"""Implementation shared by mise setup, verification, test and smoke tasks."""
import argparse
from datetime import datetime, timezone
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from environment.runtime import (
    ROOT, SUITES, activate_sources, dependency_version, git_output, load_toml,
    provenance, pythonpath, redis_executable, source_revision,
)


def run(command, **kwargs):
    return subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def sync_sources():
    """Fetch immutable revisions; never reset or discard local source edits."""
    (ROOT / "repos").mkdir(exist_ok=True)
    for name, pin in load_toml("repos.lock.toml").items():
        if name not in ("celery", "kombu") or not re.fullmatch(r"[0-9a-f]{40}", pin["revision"]):
            raise RuntimeError(f"Invalid source pin: {name}")
        path = ROOT / "repos" / name
        if path.is_symlink() or (ROOT / "repos").is_symlink():
            raise RuntimeError("Source checkouts must be inside repos/, not symlinks")
        if not path.exists():
            run(["git", "clone", "--filter=blob:none", "--no-checkout", pin["url"], str(path)])
            fresh = True
        else:
            fresh = False
            if not (path / ".git").is_dir():
                raise RuntimeError(f"Refusing to replace non-checkout repos/{name}")
        if git_output(path, "remote", "get-url", "origin") != pin["url"]:
            raise RuntimeError(f"Unexpected origin for repos/{name}")
        # A fresh --no-checkout clone has an empty index; don't mistake it for edits.
        if not fresh and git_output(path, "status", "--porcelain"):
            raise RuntimeError(f"Dirty repos/{name}; preserve or remove your edits explicitly")
        present = subprocess.run(["git", "-C", str(path), "cat-file", "-e", f'{pin["revision"]}^{{commit}}'],
                                 capture_output=True).returncode == 0
        if not present:
            run(["git", "-C", str(path), "fetch", "--depth=1", "origin", pin["revision"]])
        run(["git", "-C", str(path), "checkout", "--detach", pin["revision"]])
        if source_revision(name) != pin["revision"]:
            raise RuntimeError(f"Revision mismatch for repos/{name}")


def check_environment():
    """Fail on environment/source drift instead of silently testing other versions."""
    config = load_toml("mise.toml")
    expected_python = (ROOT / ".python-version").read_text().strip()
    actual_python = ".".join(map(str, sys.version_info[:3]))
    if actual_python != expected_python or config["tools"]["python"] != expected_python:
        raise RuntimeError(f"Expected Python {expected_python}, got {actual_python}")
    if Path(sys.prefix).resolve() != (ROOT / ".venv").resolve():
        raise RuntimeError("Use the shared environment: mise run setup / mise run check")
    if load_toml("pyproject.toml")["project"]["requires-python"] != f"=={expected_python}":
        raise RuntimeError("Python pins disagree")
    uv_version = subprocess.check_output(["uv", "--version"], text=True).split()[1]
    if uv_version != config["tools"]["uv"]:
        raise RuntimeError(f"Wrong uv version: {uv_version}; use mise")
    run(["uv", "lock", "--check", "--offline"])
    tool_lock = load_toml("mise.lock")["tools"]
    for name, version in config["tools"].items():
        if tool_lock[name][0]["version"] != version:
            raise RuntimeError(f"Stale mise.lock entry: {name}")
    activate_sources()
    for name, pin in load_toml("repos.lock.toml").items():
        path = ROOT / "repos" / name
        if source_revision(name) != pin["revision"] or git_output(path, "status", "--porcelain"):
            raise RuntimeError(f"repos/{name} is dirty or not at its locked revision")
        if git_output(path, "remote", "get-url", "origin") != pin["url"]:
            raise RuntimeError(f"Unexpected origin for repos/{name}")
        module = importlib.import_module(name)
        if Path(module.__file__).resolve() != path / name / "__init__.py":
            raise RuntimeError(f"Wrong {name} import origin: {module.__file__}")
        if module.__version__ != pin["version"]:
            raise RuntimeError(f"Wrong {name} source version")
        try:
            importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            pass
        else:
            raise RuntimeError(f"Installed {name} distribution shadows source-only policy; run mise run sync")
    for dependency in load_toml("pyproject.toml")["project"]["dependencies"]:
        name, version = dependency.split("==")
        if dependency_version(name) != version:
            raise RuntimeError(f"Dependency mismatch: {dependency}; run mise run sync")
    # Also validate the dependencies declared by the pinned upstream sources.
    from packaging.requirements import Requirement
    for file in ("celery/requirements/default.txt", "celery/requirements/extras/redis.txt",
                 "celery/requirements/extras/gevent.txt", "kombu/requirements/default.txt",
                 "kombu/requirements/extras/redis.txt"):
        for line in (ROOT / "repos" / file).read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            requirement = Requirement(line)
            if requirement.marker is None or requirement.marker.evaluate():
                if dependency_version(requirement.name) not in requirement.specifier:
                    raise RuntimeError(f"Unsatisfied upstream requirement in {file}: {line}")
    redis_version = subprocess.check_output([str(redis_executable()), "--version"], text=True)
    if f'v={config["tools"]["redis"]} ' not in redis_version:
        raise RuntimeError(f"Wrong Redis server: {redis_version.strip()}")
    print(json.dumps(dict(python=actual_python, uv=uv_version, redis=config["tools"]["redis"],
                         imports={name: f"repos/{name}/{name}/__init__.py" for name in ("celery", "kombu")}), indent=2))


def tests():
    directories = ("environment", *dict.fromkeys(Path(p).parts[0] for p in SUITES.values()))
    for directory in directories:
        for script in sorted((ROOT / directory).glob("test-*.py")):
            print(f"\nTesting {script.relative_to(ROOT)}", flush=True)
            run([sys.executable, str(script)], env=dict(os.environ, PYTHONPATH=pythonpath(),
                                                      PYTHONDONTWRITEBYTECODE="1"))


def smoke():
    """Small explicit controls, not a replacement for the historical full matrices."""
    cases = {
        "shutdown": ["--case", "warm-late"],  # All four pools.
        "solo-requeue": [],
        "eta-prefetch": sum((["--case", f"due-{pool}-late"] for pool in ("solo", "prefork", "threads", "gevent")), []),
        "task-policy": sum((["--case", f"failure-{pool}-ack"] for pool in ("solo", "prefork", "threads", "gevent")), []) + ["--case", "lost-prefork-retry"],
        "redis-connection": sum((["--case", f"idle-reconnect-{pool}"] for pool in ("solo", "prefork", "threads", "gevent")), []) + ["--case", "ack-before-commit-dedup"],
        "redis-visibility": ["--case", "config-B", "--case", "prefork-reserved-expiry", "--case", "result-expiry-independent"],
        "solo-hardening": [],  # All seven controls.
    }
    folder = ROOT / "environment/validation" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    folder.mkdir(parents=True)
    summary = dict(provenance=provenance(), outcomes=[])
    for suite, arguments in cases.items():
        command = [sys.executable, SUITES[suite], *arguments]
        print(f"Smoke: {suite} {' '.join(arguments)}", flush=True)
        with (folder / f"{suite}.log").open("w") as log:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                    env=dict(os.environ, PYTHONPATH=pythonpath(), PYTHONDONTWRITEBYTECODE="1"))
        summary["outcomes"].append(dict(suite=suite, arguments=arguments, exit_code=result.returncode,
                                        log=str((folder / f"{suite}.log").relative_to(ROOT))))
        (folder / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        if result.returncode:
            raise RuntimeError(f"Smoke failed: see {folder.relative_to(ROOT)}/{suite}.log")
    print(f"Smoke evidence: {folder.relative_to(ROOT)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("sources", "check", "test", "smoke"))
    args = parser.parse_args()
    {"sources": sync_sources, "check": check_environment, "test": tests, "smoke": smoke}[args.action]()


if __name__ == "__main__":
    main()
