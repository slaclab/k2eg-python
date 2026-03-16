#!/usr/bin/env python3
"""Generate a Kafka-like snapshot stream on disk and replay it through dml."""

from __future__ import annotations

import argparse
import importlib.util
import json
import random
import sys
import time
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


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


def load_dml_module():
    """Load k2eg.dml with lightweight dependency stubs for local replay."""
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

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    dml_spec = importlib.util.spec_from_file_location("k2eg_dml_replay_module", ROOT / "k2eg" / "dml.py")
    dml_module = importlib.util.module_from_spec(dml_spec)
    assert dml_spec.loader is not None
    dml_spec.loader.exec_module(dml_module)
    return dml_module


class DummyExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, fn, *args):
        self.calls.append((fn, args))
        fn(*args)


def make_client(dml_module):
    client = dml_module.dml.__new__(dml_module.dml)
    client._dml__thread = None
    client._dml__broker = None
    client._dml__consume_data = False
    return client


def make_snapshot(dml_module, pv_names, delivered):
    snapshot = dml_module.Snapshot(
        handler=lambda topic, data: delivered.append((topic, data)),
        pv_list=pv_names,
    )
    snapshot.init()
    return snapshot


def build_iteration_records(
    *,
    iteration: int,
    pv_names: list[str],
    samples_per_pv: int,
    random_seed: int,
    base_timestamp_ns: int,
) -> list[dict[str, Any]]:
    """Create one ordered iteration before transport-level reordering."""
    randomizer = random.Random(random_seed + iteration)
    records: list[dict[str, Any]] = [
        {
            "message_type": 0,
            "iter_index": iteration,
            "msg_seq": 1,
            "timestamp": base_timestamp_ns,
        }
    ]

    msg_seq = 2
    for sample_index in range(samples_per_pv):
        for pv_name in pv_names:
            records.append(
                {
                    "message_type": 1,
                    "iter_index": iteration,
                    "msg_seq": msg_seq,
                    "timestamp": base_timestamp_ns + sample_index,
                    pv_name: {
                        "value": round(randomizer.random() * 1000.0, 6),
                        "sample": sample_index,
                    },
                }
            )
            msg_seq += 1

    records.append(
        {
            "message_type": 2,
            "iter_index": iteration,
            "msg_seq": msg_seq,
            "total_messages": msg_seq,
            "timestamp": base_timestamp_ns + samples_per_pv + 1,
        }
    )
    return records


def reorder_iteration_data(records: list[dict[str, Any]], disorder_window: int, random_seed: int) -> list[dict[str, Any]]:
    """Reorder only data messages to mimic parallel dispatch while keeping one tail."""
    if disorder_window <= 1:
        return records

    rng = random.Random(random_seed)
    header = records[0]
    tail = records[-1]
    data_records = records[1:-1]
    reordered: list[dict[str, Any]] = []

    for start in range(0, len(data_records), disorder_window):
        chunk = data_records[start:start + disorder_window]
        rng.shuffle(chunk)
        reordered.extend(chunk)

    return [header, *reordered, tail]


def interleave_iterations(
    per_iteration_records: list[list[dict[str, Any]]],
    overlap_ratio: float,
) -> list[dict[str, Any]]:
    """Overlap the next iteration before the current one finishes."""
    if len(per_iteration_records) <= 1 or overlap_ratio <= 0:
        return [record for records in per_iteration_records for record in records]

    merged: list[dict[str, Any]] = []
    for index, records in enumerate(per_iteration_records[:-1]):
        next_records = per_iteration_records[index + 1]
        split_at = max(1, min(len(records) - 1, int(len(records) * (1.0 - overlap_ratio))))
        overlap_count = max(1, min(len(next_records) - 1, int(len(next_records) * overlap_ratio)))
        merged.extend(records[:split_at])
        merged.extend(next_records[:overlap_count])
        per_iteration_records[index + 1] = next_records[overlap_count:]
        merged.extend(records[split_at:])
    merged.extend(per_iteration_records[-1])
    return merged


def write_stream(stream_path: Path, records: list[dict[str, Any]]):
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    with stream_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def replay_stream(dml_module, stream_path: Path, pv_names: list[str], topic: str):
    delivered: list[tuple[str, dict[str, Any]]] = []
    executor = DummyExecutor()
    client = make_client(dml_module)
    snapshot = make_snapshot(dml_module, pv_names, delivered)

    replay_started = time.perf_counter()
    with stream_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            message_type = record["message_type"]
            iteration = record["iter_index"]
            if message_type == 0:
                client._dml__handle_recurring_snapshot_header(snapshot, topic, dict(record), iteration)
            elif message_type == 1:
                client._dml__handle_recurring_snapshot_data(executor, snapshot, topic, dict(record), iteration)
            elif message_type == 2:
                client._dml__handle_recurring_snapshot_tail(executor, snapshot, topic, dict(record), iteration)
    replay_elapsed = time.perf_counter() - replay_started
    return delivered, replay_elapsed


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pv-count", type=int, default=100, help="Number of PVs in the simulated snapshot.")
    parser.add_argument("--hz", type=int, default=120, help="Samples per PV per second.")
    parser.add_argument("--duration-sec", type=float, default=1.0, help="Simulation duration for each iteration.")
    parser.add_argument("--iterations", type=int, default=2, help="Number of recurring snapshot iterations to generate.")
    parser.add_argument("--disorder-window", type=int, default=32, help="Shuffle window size inside one iteration.")
    parser.add_argument("--overlap-ratio", type=float, default=0.10, help="Fraction of the next iteration delivered before the current one closes.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed.")
    parser.add_argument(
        "--stream-file",
        type=Path,
        default=ROOT / "tmp" / "snapshot-replay-stream.jsonl",
        help="Path to the generated replay stream.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dml_module = load_dml_module()
    pv_names = [f"pv:{index:03d}" for index in range(args.pv_count)]
    samples_per_pv = max(1, int(args.hz * args.duration_sec))
    expected_values_per_iteration = args.pv_count * samples_per_pv

    generation_started = time.perf_counter()
    per_iteration_records: list[list[dict[str, Any]]] = []
    for iteration in range(1, args.iterations + 1):
        ordered_records = build_iteration_records(
            iteration=iteration,
            pv_names=pv_names,
            samples_per_pv=samples_per_pv,
            random_seed=args.seed,
            base_timestamp_ns=iteration * 1_000_000_000,
        )
        reordered_records = reorder_iteration_data(ordered_records, args.disorder_window, args.seed + iteration * 1000)
        per_iteration_records.append(reordered_records)

    replay_records = interleave_iterations(per_iteration_records, args.overlap_ratio)
    write_stream(args.stream_file, replay_records)
    generation_elapsed = time.perf_counter() - generation_started

    delivered, replay_elapsed = replay_stream(dml_module, args.stream_file, pv_names, "snapshot-replay")
    total_data_messages = expected_values_per_iteration * args.iterations
    total_stream_messages = len(replay_records)

    if len(delivered) != args.iterations:
        raise RuntimeError(f"Expected {args.iterations} delivered iterations, got {len(delivered)}")

    for index, (_, payload) in enumerate(delivered, start=1):
        value_count = sum(len(payload[pv_name]) for pv_name in pv_names)
        if value_count != expected_values_per_iteration:
            raise RuntimeError(
                f"Iteration {index} expected {expected_values_per_iteration} values, got {value_count}"
            )

    print("Simulation complete")
    print(f"  stream_file: {args.stream_file}")
    print(f"  pv_count: {args.pv_count}")
    print(f"  hz: {args.hz}")
    print(f"  duration_sec: {args.duration_sec}")
    print(f"  iterations: {args.iterations}")
    print(f"  disorder_window: {args.disorder_window}")
    print(f"  overlap_ratio: {args.overlap_ratio:.2f}")
    print(f"  samples_per_pv: {samples_per_pv}")
    print(f"  data_messages: {total_data_messages}")
    print(f"  stream_messages: {total_stream_messages}")
    print(f"  generation_sec: {generation_elapsed:.6f}")
    print(f"  replay_sec: {replay_elapsed:.6f}")
    print(f"  replay_us_per_data_message: {(replay_elapsed * 1_000_000) / total_data_messages:.3f}")
    print(f"  replay_messages_per_sec: {total_stream_messages / replay_elapsed:.0f}")


if __name__ == "__main__":
    main()
