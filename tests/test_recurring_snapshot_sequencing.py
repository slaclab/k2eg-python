import importlib.util
import sys
import types
from pathlib import Path


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
    snapshot.init()
    return snapshot


def make_client():
    client = dml_module.dml.__new__(dml_module.dml)
    client._dml__thread = None
    client._dml__broker = None
    client._dml__consume_data = False
    return client


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
