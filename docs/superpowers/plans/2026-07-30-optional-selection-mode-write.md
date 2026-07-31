# Optional selection_mode Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow one image to write snapshot events to schemas with or without `lesson_snapshot_event.selection_mode`.

**Architecture:** Add a default-enabled boolean to application configuration and pass it into the database repository. The repository selects one of two explicit INSERT statements, so disabling the option removes the column from every SQL clause and parameter map.

**Tech Stack:** Python 3.11, TOML, PyMySQL, unittest/pytest

---

### Task 1: Add and document the configuration switch

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/config.toml.example`
- Test: `tests/test_vision_orchestrator_config.py`

- [ ] **Step 1: Write failing configuration tests**

Add `WriteSnapshotSelectionMode = false` to the TOML fixture and assert:

```python
self.assertFalse(config.write_snapshot_selection_mode)
self.assertTrue(VisionOrchestratorConfig().write_snapshot_selection_mode)
```

- [ ] **Step 2: Run the focused test and verify RED**

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_config.py
```

Expected: failure because `write_snapshot_selection_mode` does not exist.

- [ ] **Step 3: Implement and document the configuration field**

Add to `VisionOrchestratorConfig`:

```python
write_snapshot_selection_mode: bool = True
```

Load `WriteSnapshotSelectionMode` through `_to_bool`. Add this documented example:

```toml
# true：数据库包含 selection_mode 时固定写入 1；false：兼容没有该字段的旧表。
WriteSnapshotSelectionMode = true
```

- [ ] **Step 4: Run the configuration tests and verify GREEN**

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_config.py
```

Expected: all configuration tests pass.

### Task 2: Select snapshot SQL by configuration

**Files:**
- Modify: `app/application/factories.py`
- Modify: `app/infrastructure/db/repositories.py`
- Test: `tests/test_vision_orchestrator_repositories.py`

- [ ] **Step 1: Write a failing repository compatibility test**

Construct `VisionOrchestratorRepository(connection, write_snapshot_selection_mode=False)`, insert one snapshot row, and assert:

```python
self.assertNotIn("selection_mode", sql)
self.assertNotIn("selection_mode", params[0])
```

- [ ] **Step 2: Run the repository test and verify RED**

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_repositories.py
```

Expected: failure because the constructor does not accept the option.

- [ ] **Step 3: Implement repository branching and factory wiring**

Change the constructor to:

```python
def __init__(self, connection, write_snapshot_selection_mode: bool = True):
    self.connection = connection
    self.write_snapshot_selection_mode = bool(write_snapshot_selection_mode)
```

Only add `params["selection_mode"] = 1` and use the current selection-mode SQL when enabled. When disabled, use an explicit INSERT and duplicate-key update without that field. In `build_worker`, pass `config.write_snapshot_selection_mode` into the repository.

- [ ] **Step 4: Run focused and full verification**

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_config.py tests/test_vision_orchestrator_repositories.py
conda run -n jy-tias python -m pytest -q
git diff --check
```

Expected: all tests pass and the diff check exits successfully.
