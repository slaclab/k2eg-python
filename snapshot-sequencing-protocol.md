# Snapshot Sequencing Protocol — `msg_seq` / `total_messages`

## Context

k2eg repeating snapshots emit a chain of three message types per iteration:

```
Header (message_type=0)  →  Data (message_type=1) × N  →  Tail (message_type=2)
```

All three message types are published to the same topic (`snapshot_name`).
Previously there was no way for a client to know the order or count of messages
within an iteration. This update adds two sequence fields to solve that.

---

## New Fields

### `msg_seq` — present in Header, Data, and Tail

| Message type | Value |
|---|---|
| Header | Always `1` |
| Data   | `2`, `3`, … (one per PV event, assigned atomically; order within Data messages is not guaranteed) |
| Tail   | `N` where N = total number of messages in the iteration |

- Type: `uint64`
- Monotonically increasing within one iteration, starting at 1
- Resets to 1 for every new iteration (`iter_index`)

### `total_messages` — present in Tail only

- Type: `uint64`
- Equals the Tail's own `msg_seq`
- Tells the client exactly how many messages (Header + all Data + Tail) belong to
  the current iteration
- Formula: `total_messages = 1 (Header) + number_of_pv_events (Data) + 1 (Tail)`

---

## Message Shapes (Msgpack map keys)

### Header (`message_type = 0`)
```
{
  "message_type": 0,
  "snapshot_name": <string>,
  "timestamp":     <int64, unix ns>,
  "iter_index":    <int64>,
  "msg_seq":       <uint64>   ← NEW, always 1
}
```

### Data (`message_type = 1`)
```
{
  "message_type": 1,
  "timestamp":    <int64, unix ns>,
  "iter_index":   <int64>,
  "msg_seq":      <uint64>   ← NEW, >= 2
  <pv_name>:      <pv data>
}
```

### Tail (`message_type = 2`)
```
{
  "message_type":   2,
  "snapshot_name":  <string>,
  "timestamp":      <int64, unix ns>,
  "iter_index":     <int64>,
  "error":          <int>,
  "error_message":  <string>   (omitted when error == 0)
  "msg_seq":        <uint64>   ← NEW, equals total_messages
  "total_messages": <uint64>   ← NEW, total count for this iteration
}
```

---

## How a Client Should Use These Fields

### Identifying iteration boundaries
- A message with `message_type=0` (`msg_seq=1`) marks the start of a new iteration.
- A message with `message_type=2` marks the end. At that point `total_messages` is known.

### Detecting missing messages
```
expected_count = total_messages          # from Tail
received_count = number of messages seen with this iter_index
if received_count < expected_count:
    # at least one message was lost
```

### Reordering out-of-order delivery
If the transport can reorder messages, buffer all messages for an `iter_index` until
the Tail arrives, then sort by `msg_seq` before processing:
```
sort messages by msg_seq ascending
→ Header (1), Data (2..N-1), Tail (N)
```

### Connecting mid-stream
A client that connects while an iteration is in progress will receive partial Data
messages and possibly a Tail without a preceding Header. The correct behaviour is:

1. Discard any messages until a Header (`msg_seq=1`) is seen.
2. Start collecting from that Header onwards.
3. Use `total_messages` from the subsequent Tail to validate completeness.

---

## Backward Compatibility

Old clients that do not read `msg_seq` or `total_messages` are unaffected.
Both fields are simply additional keys in the Msgpack/JSON map; unknown keys
are ignored by map-based decoders.

The `respect_push_order` command option has been removed from the server.
Server-side ordered delivery is no longer supported; clients are expected to
use `msg_seq` for alignment instead.

---

## Invariants Guaranteed by the Server

| Invariant | Guarantee |
|---|---|
| Header is always `msg_seq=1` | Yes — Header always acquires the counter first |
| Tail is always last | Yes — server calls `waitDataDrained` before publishing Tail |
| `total_messages` == Tail's `msg_seq` | Yes — same value assigned once |
| Data `msg_seq` values are unique within an iteration | Yes — atomic increment |
| Data `msg_seq` values are contiguous | **Not guaranteed** — gaps can appear if a PV event is dropped upstream |
| Data messages arrive in `msg_seq` order | **Not guaranteed** — concurrent thread pool dispatch |
