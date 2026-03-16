import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest


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

dml_spec = importlib.util.spec_from_file_location("k2eg_dml_test_module", ROOT / "k2eg" / "dml.py")
dml_module = importlib.util.module_from_spec(dml_spec)
assert dml_spec.loader is not None
dml_spec.loader.exec_module(dml_module)


class DummyExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, fn, *args):
        self.calls.append((fn, args))
        fn(*args)


def make_snapshot(handler):
    snapshot = dml_module.Snapshot(handler=handler, pv_list=["pv:a", "pv:b"])
    snapshot.properties = types.SimpleNamespace(sub_push_delay_msec=0)
    snapshot.init()
    return snapshot


def make_buffered_snapshot(handler):
    snapshot = make_snapshot(handler)
    snapshot.properties = types.SimpleNamespace(sub_push_delay_msec=100)
    return snapshot


def make_client():
    client = dml_module.dml.__new__(dml_module.dml)
    client._dml__thread = None
    client._dml__broker = None
    client._dml__consume_data = False
    return client


def run_iteration_sequence(client, executor, snapshot, topic, iteration, events):
    client._dml__handle_recurring_snapshot_header(
        snapshot,
        topic,
        {"timestamp": iteration * 1000, "msg_seq": 1},
        iteration,
    )
    for msg_seq, pv_name, value in events:
        client._dml__handle_recurring_snapshot_data(
            executor,
            snapshot,
            topic,
            {
                "timestamp": iteration * 1000 + msg_seq,
                "iter_index": iteration,
                "message_type": 1,
                "msg_seq": msg_seq,
                pv_name: value,
            },
            iteration,
        )
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        topic,
        {
            "timestamp": iteration * 1000 + len(events) + 1,
            "iter_index": iteration,
            "message_type": 2,
            "msg_seq": len(events) + 2,
            "total_messages": len(events) + 2,
        },
        iteration,
    )


def run_buffered_iteration_sequence(client, executor, snapshot, topic, iteration, events, header_submission_seq=1, tail_submission_seq=None):
    client._dml__handle_recurring_snapshot_header(
        snapshot,
        topic,
        {"timestamp": iteration * 1000, "msg_seq": 1, "submission_seq": header_submission_seq},
        iteration,
    )
    for msg_seq, submission_seq, pv_name, value in events:
        client._dml__handle_recurring_snapshot_data(
            executor,
            snapshot,
            topic,
            {
                "timestamp": iteration * 1000 + msg_seq,
                "iter_index": iteration,
                "message_type": 1,
                "msg_seq": msg_seq,
                "submission_seq": submission_seq,
                pv_name: value,
            },
            iteration,
        )
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        topic,
        {
            "timestamp": iteration * 1000 + len(events) + 1,
            "iter_index": iteration,
            "message_type": 2,
            "msg_seq": len(events) + 2,
            "total_messages": len(events) + 2,
            "submission_seq": tail_submission_seq if tail_submission_seq is not None else header_submission_seq,
        },
        iteration,
    )


def test_recurring_snapshot_reorders_within_iteration_using_msg_seq():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "snap-topic",
        {"timestamp": 100, "msg_seq": 1},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 101, "iter_index": 1, "message_type": 1, "msg_seq": 3, "pv:a": "third"},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 102, "iter_index": 1, "message_type": 1, "msg_seq": 2, "pv:a": "second"},
        1,
    )
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 103, "iter_index": 1, "message_type": 2, "msg_seq": 4, "total_messages": 4},
        1,
    )

    assert len(delivered) == 1
    assert delivered[0][0] == "snap-topic"
    assert delivered[0][1]["iteration"] == 1
    assert delivered[0][1]["pv:a"] == ["second", "third"]
    assert delivered[0][1]["pv:b"] == []
    assert snapshot.active_iteration is None
    assert snapshot.next_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


def test_recurring_snapshot_caches_next_iteration_until_current_finishes():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "snap-topic",
        {"timestamp": 200, "msg_seq": 1},
        10,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 201, "iter_index": 10, "message_type": 1, "msg_seq": 2, "pv:a": "iter10"},
        10,
    )
    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "snap-topic",
        {"timestamp": 300, "msg_seq": 1},
        11,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 301, "iter_index": 11, "message_type": 1, "msg_seq": 2, "pv:b": "iter11"},
        11,
    )

    assert len(delivered) == 0
    assert snapshot.next_iteration is not None
    assert snapshot.next_iteration.iteration == 11

    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 202, "iter_index": 10, "message_type": 2, "msg_seq": 3, "total_messages": 3},
        10,
    )

    assert len(delivered) == 1
    assert delivered[0][1]["iteration"] == 10
    assert delivered[0][1]["pv:a"] == ["iter10"]
    assert snapshot.active_iteration is not None
    assert snapshot.active_iteration.iteration == 11
    assert snapshot.active_iteration.results["pv:b"] == ["iter11"]

    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        "snap-topic",
        {"timestamp": 302, "iter_index": 11, "message_type": 2, "msg_seq": 3, "total_messages": 3},
        11,
    )

    assert len(delivered) == 2
    assert delivered[1][1]["iteration"] == 11
    assert delivered[1][1]["pv:b"] == ["iter11"]
    assert snapshot.active_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


def test_buffered_recurring_snapshot_orders_by_submission_seq():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    run_buffered_iteration_sequence(
        client,
        executor,
        snapshot,
        "buffered-topic",
        1,
        [
            (4, 2, "pv:a", "batch2-first"),
            (2, 1, "pv:a", "batch1-first"),
            (3, 1, "pv:a", "batch1-second"),
            (5, 2, "pv:a", "batch2-second"),
        ],
        header_submission_seq=1,
        tail_submission_seq=2,
    )

    assert len(delivered) == 1
    assert delivered[0][1]["iteration"] == 1
    assert delivered[0][1]["pv:a"] == [
        "batch1-first",
        "batch1-second",
        "batch2-first",
        "batch2-second",
    ]
    assert snapshot.active_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


def test_buffered_recurring_snapshot_drops_iteration_without_submission_seq():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "buffered-topic",
        {"timestamp": 100, "msg_seq": 1},
        1,
    )

    assert delivered == []
    assert snapshot.active_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


def test_buffered_recurring_snapshot_rejects_non_monotonic_msg_seq_within_submission():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "buffered-topic",
        {"timestamp": 100, "msg_seq": 1, "submission_seq": 1},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 101, "iter_index": 1, "message_type": 1, "msg_seq": 3, "submission_seq": 1, "pv:a": "ok"},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 102, "iter_index": 1, "message_type": 1, "msg_seq": 2, "submission_seq": 1, "pv:a": "bad"},
        1,
    )

    assert delivered == []
    assert snapshot.active_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


def test_buffered_recurring_snapshot_waits_for_late_data_after_tail():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "buffered-topic",
        {"timestamp": 100, "msg_seq": 1, "submission_seq": 1},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 101, "iter_index": 1, "message_type": 1, "msg_seq": 2, "submission_seq": 1, "pv:a": "first"},
        1,
    )
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 102, "iter_index": 1, "message_type": 2, "msg_seq": 4, "total_messages": 4, "submission_seq": 1},
        1,
    )

    assert delivered == []
    assert snapshot.active_iteration is not None

    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 103, "iter_index": 1, "message_type": 1, "msg_seq": 3, "submission_seq": 1, "pv:a": "second"},
        1,
    )

    assert len(delivered) == 1
    assert delivered[0][1]["pv:a"] == ["first", "second"]
    assert snapshot.active_iteration is None


def test_buffered_recurring_snapshot_drops_iteration_when_tail_submission_seq_is_missing():
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))

    client._dml__handle_recurring_snapshot_header(
        snapshot,
        "buffered-topic",
        {"timestamp": 100, "msg_seq": 1, "submission_seq": 1},
        1,
    )
    client._dml__handle_recurring_snapshot_data(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 101, "iter_index": 1, "message_type": 1, "msg_seq": 2, "submission_seq": 1, "pv:a": "value"},
        1,
    )
    client._dml__handle_recurring_snapshot_tail(
        executor,
        snapshot,
        "buffered-topic",
        {"timestamp": 102, "iter_index": 1, "message_type": 2, "msg_seq": 3, "total_messages": 3},
        1,
    )

    assert delivered == []
    assert snapshot.active_iteration is None
    assert snapshot.state == dml_module.SnapshotState.INITIALIZED


@pytest.mark.parametrize(
    ("scenario_name", "runner"),
    [
        (
            "in_order",
            lambda client, executor, snapshot, delivered: run_iteration_sequence(
                client,
                executor,
                snapshot,
                "perf-topic",
                1,
                [(seq, "pv:a", f"value-{seq - 1}") for seq in range(2, 502)],
            ),
        ),
        (
            "intra_iteration_reorder",
            lambda client, executor, snapshot, delivered: run_iteration_sequence(
                client,
                executor,
                snapshot,
                "perf-topic",
                1,
                [(seq, "pv:a", f"value-{seq}") for seq in range(251, 502)]
                + [(seq, "pv:a", f"value-{seq}") for seq in range(2, 251)],
            ),
        ),
        (
            "iteration_overlap",
            lambda client, executor, snapshot, delivered: (
                client._dml__handle_recurring_snapshot_header(
                    snapshot,
                    "perf-topic",
                    {"timestamp": 1000, "msg_seq": 1},
                    1,
                ),
                [
                    client._dml__handle_recurring_snapshot_data(
                        executor,
                        snapshot,
                        "perf-topic",
                        {
                            "timestamp": 1000 + seq,
                            "iter_index": 1,
                            "message_type": 1,
                            "msg_seq": seq,
                            "pv:a": f"iter1-{seq}",
                        },
                        1,
                    )
                    for seq in range(2, 252)
                ],
                client._dml__handle_recurring_snapshot_header(
                    snapshot,
                    "perf-topic",
                    {"timestamp": 2000, "msg_seq": 1},
                    2,
                ),
                [
                    client._dml__handle_recurring_snapshot_data(
                        executor,
                        snapshot,
                        "perf-topic",
                        {
                            "timestamp": 2000 + seq,
                            "iter_index": 2,
                            "message_type": 1,
                            "msg_seq": seq,
                            "pv:b": f"iter2-{seq}",
                        },
                        2,
                    )
                    for seq in range(2, 252)
                ],
                [
                    client._dml__handle_recurring_snapshot_data(
                        executor,
                        snapshot,
                        "perf-topic",
                        {
                            "timestamp": 1000 + seq,
                            "iter_index": 1,
                            "message_type": 1,
                            "msg_seq": seq,
                            "pv:a": f"iter1-{seq}",
                        },
                        1,
                    )
                    for seq in range(252, 502)
                ],
                client._dml__handle_recurring_snapshot_tail(
                    executor,
                    snapshot,
                    "perf-topic",
                    {
                        "timestamp": 1505,
                        "iter_index": 1,
                        "message_type": 2,
                        "msg_seq": 502,
                        "total_messages": 502,
                    },
                    1,
                ),
                client._dml__handle_recurring_snapshot_tail(
                    executor,
                    snapshot,
                    "perf-topic",
                    {
                        "timestamp": 2505,
                        "iter_index": 2,
                        "message_type": 2,
                        "msg_seq": 252,
                        "total_messages": 252,
                    },
                    2,
                ),
            ),
        ),
        (
            "buffered_submission_reorder",
            lambda client, executor, snapshot, delivered: run_buffered_iteration_sequence(
                client,
                executor,
                snapshot,
                "perf-topic",
                1,
                [(seq + 250, 2, "pv:a", f"batch2-{seq}") for seq in range(1, 251)]
                + [(seq + 1, 1, "pv:a", f"batch1-{seq}") for seq in range(1, 251)],
                header_submission_seq=1,
                tail_submission_seq=2,
            ),
        ),
    ],
)
def test_recurring_snapshot_sequence_performance_scenarios(scenario_name, runner):
    delivered = []
    executor = DummyExecutor()
    client = make_client()
    if scenario_name == "buffered_submission_reorder":
        snapshot = make_buffered_snapshot(lambda topic, data: delivered.append((topic, data)))
    else:
        snapshot = make_snapshot(lambda topic, data: delivered.append((topic, data)))

    started = time.perf_counter()
    runner(client, executor, snapshot, delivered)
    elapsed = time.perf_counter() - started

    assert elapsed >= 0
    assert delivered, f"{scenario_name} should deliver at least one snapshot"

    if scenario_name == "in_order":
        assert delivered[0][1]["pv:a"][0] == "value-1"
        assert delivered[0][1]["pv:a"][-1] == "value-500"
    elif scenario_name == "intra_iteration_reorder":
        assert delivered[0][1]["pv:a"][0] == "value-2"
        assert delivered[0][1]["pv:a"][-1] == "value-501"
    elif scenario_name == "iteration_overlap":
        assert len(delivered) == 2
        assert delivered[0][1]["iteration"] == 1
        assert delivered[1][1]["iteration"] == 2
        assert delivered[0][1]["pv:a"][0] == "iter1-2"
        assert delivered[1][1]["pv:b"][0] == "iter2-2"
    elif scenario_name == "buffered_submission_reorder":
        assert delivered[0][1]["pv:a"][0] == "batch1-1"
        assert delivered[0][1]["pv:a"][-1] == "batch2-250"

    print(f"{scenario_name} elapsed={elapsed:.6f}s snapshots={len(delivered)}")
