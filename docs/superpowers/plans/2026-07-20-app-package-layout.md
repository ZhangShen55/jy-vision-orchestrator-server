# App Package Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Rename the Python package from `vision_orchestrator` to `app`, expose `app/main.py`, and standardize the container project root on `/app` without changing runtime contracts.

**Architecture:** This is a repository-wide namespace and path migration. Runtime behavior remains unchanged; only Python import paths, executable entrypoints, repository paths, and container paths move. A structural contract test guards the design requirements independently of optional CV/Kafka dependencies.

**Tech Stack:** Python 3.11+, FastAPI, pytest, Docker Compose, Cython

---

### Task 1: Add the package-layout contract

**Files:**
- Create: `tests/test_app_package_layout.py`

- [x] **Step 1: Write the failing structural test**

```python
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_repository_uses_app_package_layout():
    assert (ROOT / "app" / "main.py").is_file()
    assert not (ROOT / "vision_orchestrator").exists()


def test_python_sources_do_not_import_legacy_package():
    legacy_package = "vision_" + "orchestrator"
    for root in (ROOT / "app", ROOT / "tests"):
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            assert f"from {legacy_package}" not in source
            assert f"import {legacy_package}" not in source


def test_app_main_help_is_runnable():
    result = subprocess.run(
        [sys.executable, "-m", "app.main", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
```

- [x] **Step 2: Run the test and verify RED**

Run: `python -m pytest -q tests/test_app_package_layout.py`

Expected: failure because `app/main.py` does not exist and `vision_orchestrator/` still exists.

### Task 2: Migrate the Python package and imports

**Files:**
- Move: `vision_orchestrator/` to `app/`
- Move: `app/app.py` to `app/main.py`
- Modify: all `app/**/*.py`
- Modify: all `tests/**/*.py`

- [x] **Step 1: Move the package and entry module**

Run: `mv vision_orchestrator app && mv app/app.py app/main.py`

- [x] **Step 2: Replace imports and mock targets**

Replace Python package references from `vision_orchestrator` to `app`, while retaining the `vision_orchestrator` service name and Redis key values.

- [x] **Step 3: Update CLI defaults**

Set the default config path to `app/config.toml` and expose `python -m app.main`.

- [x] **Step 4: Run the layout test and verify GREEN**

Run: `python -m pytest -q tests/test_app_package_layout.py`

Expected: `3 passed`.

### Task 3: Migrate Docker and Cython paths

**Files:**
- Modify: `app/docker/Dockerfile`
- Modify: `app/docker/docker-compose.yml`
- Delete: `app/docker/env.example`
- Modify: `.gitignore`

- [x] **Step 1: Standardize the image layout**

Use `/app` as `WORKDIR`, copy the package to `/app/app`, copy the runtime config to `/app/config.toml`, and run `python -m app.main`.

- [x] **Step 2: Update protected builds**

Compile package `app`, retain `app/main.py` and other required bootstrap sources, and use `/app` as the Cython root.

- [x] **Step 3: Update Compose mounts and Dockerfile paths**

Use `app/docker/Dockerfile`, mount `../config.toml` at `/app/config.toml`, and run API/Worker commands with `app.main`.

### Task 4: Update repository documentation

**Files:**
- Modify: `README.md`
- Modify: `app/RUNNING.md`
- Modify: `app/docker/README.md`

- [x] **Step 1: Update source and config paths**

Replace package paths and commands with `app/`, `app/config.toml`, `app/docker/`, and `python -m app.main`.

- [x] **Step 2: Update container paths**

Replace `/workspace/vision_orchestrator/config.toml` with `/app/config.toml` while retaining image, container, API, Worker, and Redis names.

### Task 5: Verify every design criterion

**Files:**
- Test: `tests/test_app_package_layout.py`
- Test: `tests/`

- [x] **Step 1: Check legacy paths and imports**

Run: `test ! -d vision_orchestrator && ! rg -n 'from vision_orchestrator|import vision_orchestrator' app tests`

- [x] **Step 2: Check the entrypoint and config**

Run: `python -m app.main --help && python -c 'from app.core.config import load_vision_orchestrator_config; load_vision_orchestrator_config("app/config.toml.example")'`

- [x] **Step 3: Validate Docker Compose**

Run: `docker compose -f app/docker/docker-compose.yml config`

- [x] **Step 4: Run automated tests**

Run: `python -m pytest -q`

Expected: all tests pass when the declared dependencies are installed; otherwise report dependency-related collection failures separately from assertion failures.

### Task 6: Move shared bootstrap modules into core

**Files:**
- Create: `app/core/__init__.py`
- Move: `app/config.py` to `app/core/config.py`
- Move: `app/config_loader.py` to `app/core/config_loader.py`
- Move: `app/http_app.py` to `app/core/bootstrap.py`
- Modify: `app/main.py`
- Modify: `app/**/*.py`
- Modify: `tests/**/*.py`
- Modify: `app/docker/Dockerfile`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-20-app-package-layout-design.md`
- Test: `tests/test_app_package_layout.py`

- [x] **Step 1: Extend the layout contract**

```python
def test_repository_uses_app_package_layout():
    assert (ROOT / "app" / "__init__.py").is_file()
    assert (ROOT / "app" / "main.py").is_file()
    assert (ROOT / "app" / "core" / "__init__.py").is_file()
    assert (ROOT / "app" / "core" / "config.py").is_file()
    assert (ROOT / "app" / "core" / "config_loader.py").is_file()
    assert (ROOT / "app" / "core" / "bootstrap.py").is_file()
    assert not (ROOT / "app" / "config.py").exists()
    assert not (ROOT / "app" / "config_loader.py").exists()
    assert not (ROOT / "app" / "http_app.py").exists()
```

- [x] **Step 2: Run the test and verify RED**

Run: `python -m pytest -q tests/test_app_package_layout.py`

Expected: failure because the three modules still live directly under `app/`.

- [x] **Step 3: Move modules and update imports**

Use `app.core.config`, `app.core.config_loader`, and `app.core.bootstrap` throughout application code, tests, and mock targets.

- [x] **Step 4: Update build and design paths**

Keep `app/core/config_loader.py` as Cython bootstrap source, exclude `app/core/bootstrap.py`, and document `core/` as the location for shared configuration and dependency assembly.

- [x] **Step 5: Run the layout and full test suites**

Run: `python -m pytest -q tests/test_app_package_layout.py && python -m pytest -q`

Expected: all tests pass.
