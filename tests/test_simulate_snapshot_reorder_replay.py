import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "simulate_snapshot_reorder_replay.py"

tool_spec = importlib.util.spec_from_file_location("simulate_snapshot_reorder_replay_test_module", TOOL_PATH)
tool_module = importlib.util.module_from_spec(tool_spec)
assert tool_spec.loader is not None
sys.modules[tool_spec.name] = tool_module
tool_spec.loader.exec_module(tool_module)


def test_build_buffered_iteration_records_adds_submission_seq():
    records = tool_module.build_buffered_iteration_records(
        iteration=1,
        pv_names=["pv:a", "pv:b"],
        samples_per_pv=3,
        random_seed=7,
        base_timestamp_ns=1_000,
        submission_batch_size=2,
    )

    assert records[0]["message_type"] == 0
    assert records[0]["submission_seq"] == 1
    assert records[-1]["message_type"] == 2
    assert records[-1]["submission_seq"] >= 1
    assert all("submission_seq" in record for record in records)


def test_reorder_buffered_iteration_data_preserves_in_submission_order():
    records = tool_module.build_buffered_iteration_records(
        iteration=1,
        pv_names=["pv:a"],
        samples_per_pv=6,
        random_seed=11,
        base_timestamp_ns=1_000,
        submission_batch_size=2,
    )

    reordered = tool_module.reorder_buffered_iteration_data(records, disorder_window=3, random_seed=17)
    data_records = [record for record in reordered if record["message_type"] == 1]
    submission_to_msg_seq = {}
    for record in data_records:
        submission_to_msg_seq.setdefault(record["submission_seq"], []).append(record["msg_seq"])

    assert reordered[0]["message_type"] == 0
    assert reordered[-1]["message_type"] == 2
    for msg_seq_list in submission_to_msg_seq.values():
        assert msg_seq_list == sorted(msg_seq_list)


def test_split_iteration_records_keeps_submission_batches_intact():
    records = tool_module.build_buffered_iteration_records(
        iteration=1,
        pv_names=["pv:a"],
        samples_per_pv=6,
        random_seed=13,
        base_timestamp_ns=1_000,
        submission_batch_size=2,
    )

    prefix, suffix = tool_module._split_iteration_records(records, split_at_data_count=3)
    prefix_submission_values = {
        record["submission_seq"]
        for record in prefix
        if record.get("message_type") == 1
    }
    suffix_submission_values = {
        record["submission_seq"]
        for record in suffix
        if record.get("message_type") == 1
    }

    assert prefix_submission_values.isdisjoint(suffix_submission_values)


def test_split_iteration_records_supports_partial_segments_without_header():
    records = tool_module.build_buffered_iteration_records(
        iteration=1,
        pv_names=["pv:a"],
        samples_per_pv=6,
        random_seed=19,
        base_timestamp_ns=1_000,
        submission_batch_size=2,
    )

    _, suffix = tool_module._split_iteration_records(records, split_at_data_count=2)
    prefix_again, suffix_again = tool_module._split_iteration_records(suffix, split_at_data_count=2)

    prefix_submission_values = {
        record["submission_seq"]
        for record in prefix_again
        if record.get("message_type") == 1
    }
    suffix_submission_values = {
        record["submission_seq"]
        for record in suffix_again
        if record.get("message_type") == 1
    }

    assert prefix_submission_values.isdisjoint(suffix_submission_values)


def test_interleave_buffered_iterations_keeps_one_header_and_tail_per_iteration():
    first = tool_module.build_buffered_iteration_records(
        iteration=1,
        pv_names=["pv:a"],
        samples_per_pv=8,
        random_seed=23,
        base_timestamp_ns=1_000,
        submission_batch_size=2,
    )
    second = tool_module.build_buffered_iteration_records(
        iteration=2,
        pv_names=["pv:a"],
        samples_per_pv=8,
        random_seed=29,
        base_timestamp_ns=2_000,
        submission_batch_size=2,
    )

    merged = tool_module.interleave_buffered_iterations([first, second], overlap_ratio=0.25)
    headers = [record for record in merged if record["message_type"] == 0]
    tails = [record for record in merged if record["message_type"] == 2]

    assert [header["iter_index"] for header in headers] == [1, 2]
    assert [tail["iter_index"] for tail in tails] == [1, 2]
