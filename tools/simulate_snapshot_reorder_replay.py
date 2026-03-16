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
from dataclasses import dataclass
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
    snapshot.properties = types.SimpleNamespace(sub_push_delay_msec=0)
    snapshot.init()
    return snapshot


def make_buffered_snapshot(dml_module, pv_names, delivered, sub_push_delay_msec: int):
    snapshot = make_snapshot(dml_module, pv_names, delivered)
    snapshot.properties = types.SimpleNamespace(sub_push_delay_msec=sub_push_delay_msec)
    return snapshot


@dataclass
class ReplayMetrics:
    delivered_iterations: int
    delivered_values: int
    expected_values: int
    replay_elapsed: float


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


def build_buffered_iteration_records(
    *,
    iteration: int,
    pv_names: list[str],
    samples_per_pv: int,
    random_seed: int,
    base_timestamp_ns: int,
    submission_batch_size: int,
) -> list[dict[str, Any]]:
    """Create one ordered buffered iteration with submission_seq batches."""
    randomizer = random.Random(random_seed + iteration)
    records: list[dict[str, Any]] = [
        {
            "message_type": 0,
            "iter_index": iteration,
            "msg_seq": 1,
            "submission_seq": 1,
            "timestamp": base_timestamp_ns,
        }
    ]

    msg_seq = 2
    submission_seq = 1
    messages_in_submission = 0
    pending: dict[str, Any] | None = None  # last record of the current batch, not yet appended
    for sample_index in range(samples_per_pv):
        for pv_name in pv_names:
            if messages_in_submission >= submission_batch_size:
                # Close the current batch: mark its last record before appending
                if pending is not None:
                    pending["last_submission_data"] = True
                    records.append(pending)
                    pending = None
                submission_seq += 1
                messages_in_submission = 0
            elif pending is not None:
                records.append(pending)
                pending = None
            pending = {
                "message_type": 1,
                "iter_index": iteration,
                "msg_seq": msg_seq,
                "submission_seq": submission_seq,
                "timestamp": base_timestamp_ns + sample_index,
                pv_name: {
                    "value": round(randomizer.random() * 1000.0, 6),
                    "sample": sample_index,
                    "submission_seq": submission_seq,
                },
            }
            msg_seq += 1
            messages_in_submission += 1
    # Close the final batch
    if pending is not None:
        pending["last_submission_data"] = True
        records.append(pending)

    records.append(
        {
            "message_type": 2,
            "iter_index": iteration,
            "msg_seq": msg_seq,
            "total_messages": msg_seq,
            "submission_seq": submission_seq,
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


def reorder_buffered_iteration_data(records: list[dict[str, Any]], disorder_window: int, random_seed: int) -> list[dict[str, Any]]:
    """Reorder buffered snapshot data by submission chunks while keeping in-batch order."""
    if disorder_window <= 1:
        return records

    rng = random.Random(random_seed)
    header = records[0]
    tail = records[-1]
    data_records = records[1:-1]
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_submission_seq = None

    for record in data_records:
        record_submission_seq = record.get("submission_seq")
        if current_submission_seq is None:
            current_submission_seq = record_submission_seq
        if record_submission_seq != current_submission_seq:
            chunks.append(current_chunk)
            current_chunk = []
            current_submission_seq = record_submission_seq
        current_chunk.append(record)

    if current_chunk:
        chunks.append(current_chunk)

    reordered: list[dict[str, Any]] = []
    for start in range(0, len(chunks), disorder_window):
        window = chunks[start:start + disorder_window]
        rng.shuffle(window)
        for chunk in window:
            reordered.extend(chunk)

    return [header, *reordered, tail]


def _group_submission_chunks(records: list[dict[str, Any]]) -> tuple[dict[str, Any], list[list[dict[str, Any]]], dict[str, Any]]:
    """Split one full buffered iteration into header, submission chunks, and tail."""
    header = records[0]
    tail = records[-1]
    data_records = records[1:-1]
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_submission_seq = None

    for record in data_records:
        record_submission_seq = record.get("submission_seq")
        if current_submission_seq is None:
            current_submission_seq = record_submission_seq
        if record_submission_seq != current_submission_seq:
            chunks.append(current_chunk)
            current_chunk = []
            current_submission_seq = record_submission_seq
        current_chunk.append(record)

    if current_chunk:
        chunks.append(current_chunk)

    return header, chunks, tail


def _flatten_submission_segment(
    header: dict[str, Any] | None,
    chunks: list[list[dict[str, Any]]],
    tail: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    if header is not None:
        flattened.append(header)
    for chunk in chunks:
        flattened.extend(chunk)
    if tail is not None:
        flattened.append(tail)
    return flattened


def _split_iteration_records(records: list[dict[str, Any]], split_at_data_count: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split one iteration segment without cutting through a buffered submission batch."""
    if not records:
        return [], []

    has_header = records[0].get("message_type") == 0
    has_tail = records[-1].get("message_type") == 2
    header = records[0] if has_header else None
    tail = records[-1] if has_tail else None
    data_start = 1 if has_header else 0
    data_end = len(records) - 1 if has_tail else len(records)
    data_records = records[data_start:data_end]

    if not data_records:
        return records, []

    if split_at_data_count <= 0:
        prefix = [header] if header is not None else []
        suffix = [*data_records]
        if tail is not None:
            suffix.append(tail)
        return prefix, suffix
    if split_at_data_count >= len(data_records):
        prefix = [*data_records]
        if header is not None:
            prefix.insert(0, header)
        if tail is not None:
            prefix.append(tail)
        return prefix, []

    split_index = split_at_data_count
    split_submission_seq = data_records[split_index - 1].get("submission_seq")
    while (
        split_index < len(data_records)
        and data_records[split_index].get("submission_seq") == split_submission_seq
    ):
        split_index += 1

    prefix = [*data_records[:split_index]]
    suffix = [*data_records[split_index:]]
    if header is not None:
        prefix.insert(0, header)
    if tail is not None:
        suffix.append(tail)
    return prefix, suffix


def interleave_buffered_iterations(
    per_iteration_records: list[list[dict[str, Any]]],
    overlap_ratio: float,
) -> list[dict[str, Any]]:
    """Overlap buffered iterations by whole submission batches."""
    segments = []
    for records in per_iteration_records:
        header, chunks, tail = _group_submission_chunks(records)
        segments.append(
            {
                "header": header,
                "chunks": chunks,
                "tail": tail,
                "header_emitted": False,
            }
        )

    merged: list[dict[str, Any]] = []
    for index in range(len(segments) - 1):
        current = segments[index]
        nxt = segments[index + 1]

        current_chunk_count = len(current["chunks"])
        next_chunk_count = len(nxt["chunks"])
        current_prefix_count = max(1, min(current_chunk_count, int(current_chunk_count * (1.0 - overlap_ratio))))
        next_prefix_count = max(1, min(next_chunk_count, int(next_chunk_count * overlap_ratio)))

        merged.extend(
            _flatten_submission_segment(
                None if current["header_emitted"] else current["header"],
                current["chunks"][:current_prefix_count],
                None,
            )
        )
        current["header_emitted"] = True
        current["chunks"] = current["chunks"][current_prefix_count:]

        merged.extend(
            _flatten_submission_segment(
                None if nxt["header_emitted"] else nxt["header"],
                nxt["chunks"][:next_prefix_count],
                None,
            )
        )
        nxt["header_emitted"] = True
        nxt["chunks"] = nxt["chunks"][next_prefix_count:]

        merged.extend(_flatten_submission_segment(None, current["chunks"], current["tail"]))
        current["chunks"] = []

    final_segment = segments[-1]
    merged.extend(
        _flatten_submission_segment(
            None if final_segment["header_emitted"] else final_segment["header"],
            final_segment["chunks"],
            final_segment["tail"],
        )
    )
    return merged


def interleave_iterations(
    per_iteration_records: list[list[dict[str, Any]]],
    overlap_ratio: float,
) -> list[dict[str, Any]]:
    """Overlap the next iteration before the current one finishes."""
    if len(per_iteration_records) <= 1 or overlap_ratio <= 0:
        return [record for records in per_iteration_records for record in records]

    is_buffered = any(
        record.get("message_type") == 1 and "submission_seq" in record
        for records in per_iteration_records
        for record in records
    )
    if is_buffered:
        return interleave_buffered_iterations(per_iteration_records, overlap_ratio)

    merged: list[dict[str, Any]] = []
    for index, records in enumerate(per_iteration_records[:-1]):
        next_records = per_iteration_records[index + 1]
        current_data_count = max(len(records) - 2, 0)
        next_data_count = max(len(next_records) - 2, 0)
        current_prefix_data = max(1, min(current_data_count, int(current_data_count * (1.0 - overlap_ratio))))
        next_prefix_data = max(1, min(next_data_count, int(next_data_count * overlap_ratio)))

        current_prefix, current_suffix = _split_iteration_records(records, current_prefix_data)
        next_prefix, next_suffix = _split_iteration_records(next_records, next_prefix_data)

        merged.extend(current_prefix)
        merged.extend(next_prefix)
        per_iteration_records[index + 1] = next_suffix
        merged.extend(current_suffix)
    merged.extend(per_iteration_records[-1])
    return merged


def write_stream(stream_path: Path, records: list[dict[str, Any]]):
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    with stream_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def replay_stream(dml_module, stream_path: Path, pv_names: list[str], topic: str, sub_push_delay_msec: int, expected_iterations: int = 0):
    delivered: list[tuple[str, dict[str, Any]]] = []
    executor = DummyExecutor()
    client = make_client(dml_module)
    if sub_push_delay_msec > 0:
        snapshot = make_buffered_snapshot(dml_module, pv_names, delivered, sub_push_delay_msec)
    else:
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

    # In buffered mode the merge worker runs on a daemon thread; wait for all
    # deliveries to arrive before measuring elapsed time.
    if sub_push_delay_msec > 0 and expected_iterations > 0:
        deadline = time.monotonic() + 30.0
        while len(delivered) < expected_iterations and time.monotonic() < deadline:
            time.sleep(0.005)

    replay_elapsed = time.perf_counter() - replay_started
    delivered_values = sum(
        len(payload[pv_name])
        for _, payload in delivered
        for pv_name in pv_names
    )
    return delivered, ReplayMetrics(
        delivered_iterations=len(delivered),
        delivered_values=delivered_values,
        expected_values=0,
        replay_elapsed=replay_elapsed,
    )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pv-count", type=int, default=100, help="Number of PVs in the simulated snapshot.")
    parser.add_argument("--hz", type=int, default=120, help="Samples per PV per second.")
    parser.add_argument("--duration-sec", type=float, default=1.0, help="Simulation duration for each iteration.")
    parser.add_argument("--iterations", type=int, default=2, help="Number of recurring snapshot iterations to generate.")
    parser.add_argument("--disorder-window", type=int, default=32, help="Shuffle window size inside one iteration.")
    parser.add_argument("--overlap-ratio", type=float, default=0.10, help="Fraction of the next iteration delivered before the current one closes.")
    parser.add_argument("--submission-batch-size", type=int, default=64, help="Buffered snapshot messages per submission batch.")
    parser.add_argument("--sub-push-delay-msec", type=int, default=0, help="When > 0, generate buffered recurring snapshot traffic with submission_seq.")
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
    buffered_mode = args.sub_push_delay_msec > 0

    generation_started = time.perf_counter()
    per_iteration_records: list[list[dict[str, Any]]] = []
    for iteration in range(1, args.iterations + 1):
        if buffered_mode:
            ordered_records = build_buffered_iteration_records(
                iteration=iteration,
                pv_names=pv_names,
                samples_per_pv=samples_per_pv,
                random_seed=args.seed,
                base_timestamp_ns=iteration * 1_000_000_000,
                submission_batch_size=max(1, args.submission_batch_size),
            )
            reordered_records = reorder_buffered_iteration_data(
                ordered_records,
                args.disorder_window,
                args.seed + iteration * 1000,
            )
        else:
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

    delivered, metrics = replay_stream(
        dml_module,
        args.stream_file,
        pv_names,
        "snapshot-replay",
        args.sub_push_delay_msec,
        expected_iterations=args.iterations,
    )
    total_data_messages = expected_values_per_iteration * args.iterations
    total_stream_messages = len(replay_records)
    metrics.expected_values = total_data_messages

    if len(delivered) != args.iterations:
        raise RuntimeError(f"Expected {args.iterations} delivered iterations, got {len(delivered)}")

    for index, (_, payload) in enumerate(delivered, start=1):
        value_count = sum(len(payload[pv_name]) for pv_name in pv_names)
        if value_count != expected_values_per_iteration:
            raise RuntimeError(
                f"Iteration {index} expected {expected_values_per_iteration} values, got {value_count}"
            )

    first_iteration_first_pv = delivered[0][1][pv_names[0]][0]
    last_iteration_last_pv = delivered[-1][1][pv_names[-1]][-1]

    print("Simulation complete")
    print(f"  stream_file: {args.stream_file}")
    print(f"  mode: {'buffered-submission-seq' if buffered_mode else 'msg-seq-only'}")
    print(f"  pv_count: {args.pv_count}")
    print(f"  hz: {args.hz}")
    print(f"  duration_sec: {args.duration_sec}")
    print(f"  iterations: {args.iterations}")
    print(f"  disorder_window: {args.disorder_window}")
    print(f"  overlap_ratio: {args.overlap_ratio:.2f}")
    print(f"  sub_push_delay_msec: {args.sub_push_delay_msec}")
    print(f"  submission_batch_size: {args.submission_batch_size}")
    print(f"  samples_per_pv: {samples_per_pv}")
    print(f"  data_messages: {total_data_messages}")
    print(f"  stream_messages: {total_stream_messages}")
    print(f"  delivered_iterations: {metrics.delivered_iterations}")
    print(f"  delivered_values: {metrics.delivered_values}")
    print(f"  generation_sec: {generation_elapsed:.6f}")
    print(f"  replay_sec: {metrics.replay_elapsed:.6f}")
    print(f"  replay_us_per_data_message: {(metrics.replay_elapsed * 1_000_000) / total_data_messages:.3f}")
    print(f"  replay_messages_per_sec: {total_stream_messages / metrics.replay_elapsed:.0f}")
    print(f"  first_value_marker: {json.dumps(first_iteration_first_pv, sort_keys=True)}")
    print(f"  last_value_marker: {json.dumps(last_iteration_last_pv, sort_keys=True)}")


if __name__ == "__main__":
    main()
