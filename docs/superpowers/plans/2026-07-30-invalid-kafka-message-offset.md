# Invalid Kafka Message Offset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Skip null or structurally invalid Kafka task messages without blocking later tasks.

**Architecture:** Catch `InvalidTaskMessage` at the controlled-consumer boundary, before invoking the task handler. Commit only the rejected record's `TopicPartition` at `offset + 1`, update worker failure state, and return control to the normal polling loop.

**Tech Stack:** Python 3.11, kafka-python, unittest/pytest

---

### Task 1: Handle invalid records in the controlled consumer

**Files:**
- Modify: `tests/test_vision_orchestrator_controlled_consumer.py`
- Modify: `app/infrastructure/kafka/controlled_consumer.py:1-170`

- [ ] **Step 1: Write failing regression tests**

Extend the pollable fake consumer so `commit(offsets)` records the supplied mapping. Add tests that run a null record and a record missing `teacher_video_path`; assert that the handler is not called and the committed `TopicPartition("classroom_cv_task", 0)` points to `OffsetAndMetadata(13, "")`. Add a two-poll test proving a valid message after the rejected record is handled normally.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_controlled_consumer.py
```

Expected: the new tests fail because `_parse_message()` raises before any offset commit.

- [ ] **Step 3: Implement minimal invalid-message handling**

Import `TopicPartition`, `OffsetAndMetadata`, and `InvalidTaskMessage`. Wrap parsing in `_consume_one()`:

```python
try:
    task_message = VisionOrchestratorKafkaConsumer._parse_message(raw_message)
except InvalidTaskMessage as exc:
    self.failed_count += 1
    self.last_error = str(exc)
    self._commit_message_offset(raw_message)
    self._heartbeat(
        WorkerRuntimeState.RUNNING,
        self.control_repository.get_state().desired_state,
    )
    return True
```

Implement exact partition commit:

```python
def _commit_message_offset(self, raw_message) -> None:
    topic_partition = TopicPartition(raw_message.topic, raw_message.partition)
    next_offset = OffsetAndMetadata(raw_message.offset + 1, "")
    self.consumer.commit({topic_partition: next_offset})
```

Log worker ID, topic, partition, offset, rejection reason, and raw value before committing.

- [ ] **Step 4: Run focused and full verification**

Run:

```bash
conda run -n jy-tias python -m pytest -q tests/test_vision_orchestrator_controlled_consumer.py
conda run -n jy-tias python -m pytest -q
git diff --check
```

Expected: all tests pass and `git diff --check` exits successfully.
