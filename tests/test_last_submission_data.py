"""
Tests for last_submission_data flag behaviour.

A submission batch is complete only when the server sets last_submission_data=True
on the final message of that batch.  The flag triggers early off-thread grouping
so that _merge_worker has less inline work to do at finalization time.
"""

import importlib.util
import sys
import time
import types
from pathlib import Path

# ---------------------------------------------------------------------------
# Stub out C-extension dependencies so the module can be imported without a
# running Kafka broker.
# ---------------------------------------------------------------------------

class _DummyRWLockFairD:
    pass


class _DummyKafkaError:
    _PARTITION_EOF = object()

    def code(self):
        return self._PARTITION_EOF


class _DummyConsumer:
    def __init__(self, *args, **kwargs):
        pass


class _DummyProducer:
    def __init__(self, *args, **kwargs):
        pass


readerwriterlock_module = types.ModuleType("readerwriterlock")
readerwriterlock_module.rwlock = types.SimpleNamespace(RWLockFairD=_DummyRWLockFairD)
sys.modules.setdefault("readerwriterlock", readerwriterlock_module)

confluent_kafka_module = types.ModuleType("confluent_kafka")
confluent_kafka_module.KafkaError = _DummyKafkaError
confluent_kafka_module.Consumer = _DummyConsumer
confluent_kafka_module.TopicPartition = object
confluent_kafka_module.Producer = _DummyProducer
confluent_kafka_module.OFFSET_END = 0
confluent_kafka_module.OFFSET_BEGINNING = 0
confluent_kafka_module.KafkaException = Exception
sys.modules.setdefault("confluent_kafka", confluent_kafka_module)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

dml_spec = importlib.util.spec_from_file_location("k2eg_dml_lsd_test", ROOT / "k2eg" / "dml.py")
dml_module = importlib.util.module_from_spec(dml_spec)
assert dml_spec.loader is not None
dml_spec.loader.exec_module(dml_module)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

WAIT_TIMEOUT = 2.0  # seconds to wait for the merge worker thread


class TrackingExecutor:
    """Synchronous executor that records each submit call."""

    def __init__(self):
        self.submitted = []  # list of buffer.submission_seq values

    def submit(self, fn, *args):
        # Record which buffer was submitted (first positional arg is the buffer)
        if args and hasattr(args[0], "submission_seq"):
            self.submitted.append(args[0].submission_seq)
        fn(*args)


def make_buffered_snapshot(handler):
    snapshot = dml_module.Snapshot(handler=handler, pv_list=["pv:a", "pv:b"])
    snapshot.properties = types.SimpleNamespace(sub_push_delay_msec=100)
    snapshot.init()
    return snapshot


def make_client():
    client = dml_module.dml.__new__(dml_module.dml)
    client._dml__thread = None
    client._dml__broker = None
    client._dml__consume_data = False
    return client


def send_header(client, snapshot, topic, iteration, submission_seq):
    client._dml__handle_recurring_snapshot_header(
        snapshot,
        topic,
        {"timestamp": iteration * 1000, "msg_seq": 1, "submission_seq": submission_seq},
        iteration,
    )


def send_data(client, executor, snapshot, topic, iteration, msg_seq, submission_seq, pv_name, value, last_submission_data=False):
    msg = {
        "timestamp": iteration * 1000 + msg_seq,
        "iter_index": iteration,
        "message_type": 1,
        "msg_seq": msg_seq,
        "submission_seq": submission_seq,
        pv_name: value,
    }
    if last_submission_data:
        msg["last_submission_data"] = True
    client._dml__handle_recurring_snapshot_data(executor, snapshot, topic, msg, iteration)


def send_tail(client, executor, snapshot, topic, iteration, msg_seq, total_messages, tail_submission_seq):
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        topic,
        {
            "timestamp": iteration * 1000 + msg_seq,
            "iter_index": iteration,
            "message_type": 2,
            "msg_seq": msg_seq,
            "total_messages": total_messages,
            "submission_seq": tail_submission_seq,
        },
        iteration,
    )


def wait_for_delivery(delivered_list, expected_count=1, timeout=WAIT_TIMEOUT):
    """Spin-wait until the merge worker thread delivers the expected number of snapshots."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(delivered_list) >= expected_count:
            return
        time.sleep(0.005)
    raise TimeoutError(f"Expected {expected_count} delivery(ies), got {len(delivered_list)} after {timeout}s")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_single_submission_completed_by_last_submission_data():
    """A single submission batch whose last message carries last_submission_data=True
    must be grouped eagerly (before the tail arrives) and delivered correctly."""
    delivered = []
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    send_header(client, snapshot, "t", 1, submission_seq=1)
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1, pv_name="pv:a", value="v1")
    send_data(client, executor, snapshot, "t", 1, msg_seq=3, submission_seq=1, pv_name="pv:a", value="v2",
              last_submission_data=True)

    # Buffer must have been submitted to the executor before the tail arrives
    assert 1 in executor.submitted, "Buffer should be eagerly enqueued on last_submission_data=True"

    send_tail(client, executor, snapshot, "t", 1, msg_seq=4, total_messages=4, tail_submission_seq=1)

    wait_for_delivery(delivered)
    assert delivered[0][1]["pv:a"] == ["v1", "v2"]
    assert delivered[0][1]["pv:b"] == []
    assert snapshot.active_iteration is None


def test_buffer_not_enqueued_without_last_submission_data():
    """Without last_submission_data, a buffer must NOT be submitted to the executor
    during the data phase — only the finalization sweep may enqueue it."""
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: None)

    send_header(client, snapshot, "t", 1, submission_seq=1)
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1, pv_name="pv:a", value="v1")
    send_data(client, executor, snapshot, "t", 1, msg_seq=3, submission_seq=1, pv_name="pv:a", value="v2")

    # No early enqueue expected
    assert executor.submitted == [], "Buffer must not be enqueued without last_submission_data"


def test_multiple_submissions_each_completed_by_last_submission_data():
    """Two submission batches, each signalled complete by last_submission_data=True.
    Result must be ordered by submission_seq."""
    delivered = []
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    send_header(client, snapshot, "t", 1, submission_seq=1)
    # Batch 1
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1, pv_name="pv:a", value="b1-v1")
    send_data(client, executor, snapshot, "t", 1, msg_seq=3, submission_seq=1, pv_name="pv:a", value="b1-v2",
              last_submission_data=True)
    # Batch 2
    send_data(client, executor, snapshot, "t", 1, msg_seq=4, submission_seq=2, pv_name="pv:a", value="b2-v1")
    send_data(client, executor, snapshot, "t", 1, msg_seq=5, submission_seq=2, pv_name="pv:a", value="b2-v2",
              last_submission_data=True)

    assert executor.submitted == [1, 2], "Both buffers should be eagerly enqueued"

    send_tail(client, executor, snapshot, "t", 1, msg_seq=6, total_messages=6, tail_submission_seq=2)

    wait_for_delivery(delivered)
    assert delivered[0][1]["pv:a"] == ["b1-v1", "b1-v2", "b2-v1", "b2-v2"]


def test_interleaved_submissions_completed_by_last_submission_data():
    """Messages from two submission batches arrive interleaved on the Kafka topic.
    Each batch is completed by its own last_submission_data=True.
    The final result must still be ordered by submission_seq."""
    delivered = []
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    send_header(client, snapshot, "t", 1, submission_seq=1)
    # Interleaved delivery: batch1-first, batch2-first, batch1-last, batch2-last
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1, pv_name="pv:a", value="b1-first")
    send_data(client, executor, snapshot, "t", 1, msg_seq=3, submission_seq=2, pv_name="pv:a", value="b2-first")
    send_data(client, executor, snapshot, "t", 1, msg_seq=4, submission_seq=1, pv_name="pv:b", value="b1-last",
              last_submission_data=True)
    send_data(client, executor, snapshot, "t", 1, msg_seq=5, submission_seq=2, pv_name="pv:b", value="b2-last",
              last_submission_data=True)

    assert set(executor.submitted) == {1, 2}

    send_tail(client, executor, snapshot, "t", 1, msg_seq=6, total_messages=6, tail_submission_seq=2)

    wait_for_delivery(delivered)
    result = delivered[0][1]
    # submission 1 values must precede submission 2 values
    assert result["pv:a"] == ["b1-first", "b2-first"]
    assert result["pv:b"] == ["b1-last", "b2-last"]


def test_no_double_enqueue_when_last_submission_data_and_finalization_sweep():
    """A buffer already enqueued via last_submission_data must not be submitted
    again by the finalization sweep."""
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: None)

    send_header(client, snapshot, "t", 1, submission_seq=1)
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1, pv_name="pv:a", value="v",
              last_submission_data=True)

    enqueued_before_tail = list(executor.submitted)

    send_tail(client, executor, snapshot, "t", 1, msg_seq=3, total_messages=3, tail_submission_seq=1)

    # The finalization sweep must not re-submit what was already enqueued
    assert executor.submitted == enqueued_before_tail, "Buffer must not be enqueued twice"


def test_last_submission_data_stripped_from_pv_extraction():
    """last_submission_data must be treated as metadata and not mistaken for a PV name."""
    delivered = []
    executor = TrackingExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    send_header(client, snapshot, "t", 1, submission_seq=1)
    send_data(client, executor, snapshot, "t", 1, msg_seq=2, submission_seq=1,
              pv_name="pv:a", value="ok", last_submission_data=True)
    send_tail(client, executor, snapshot, "t", 1, msg_seq=3, total_messages=3, tail_submission_seq=1)

    wait_for_delivery(delivered)
    result = delivered[0][1]
    assert "last_submission_data" not in result, "last_submission_data must be stripped as metadata"
    assert result["pv:a"] == ["ok"]
