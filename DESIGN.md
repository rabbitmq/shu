# Shu Design

## Motivation

Ra uses DETS to store per-server Raft metadata (`current_term`, `voted_for`,
`last_applied`). DETS has several problems for this use case:

- It is a general-purpose hash table with significant overhead per record.
- It uses a single process for all operations, creating a bottleneck.
- It does not distinguish between fields that need fsync and fields that don't.
- Its file format is complex and recovery is slow.
- It has a 2GB file size limit.

Shu replaces DETS with a purpose-built store that exploits the fixed-schema,
known-access-pattern nature of Raft metadata.

## Architecture

Shu is a stateful module -- no mandatory processes. The caller owns the file
descriptor and the opaque `state()` record. A single writer is assumed. The
caller (e.g. a `gen_batch_server`) is responsible for serialising writes.

```
                    ┌─────────────────────────────┐
                    │  Caller (gen_batch_server)   │
                    │  owns shu:state()            │
                    └──────────┬──────────────────-┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
         write/3,4        read/3           prepare_compact/1
         write_batch/2    read_all/2       do_compact/1  ← can run
         delete/2                          finish_compact/2  in any process
              │                │                │
              ▼                ▼                ▼
    ┌─────────────────────────────────────────────────┐
    │                  shu.erl                        │
    │  #shu{} state: fd, key_to_slot, wal_tab, ...    │
    └──────────┬──────────────────────────────────────┘
               │
               ▼
    ┌─────────────────────────────────────────────────┐
    │              Single .shu file                    │
    │  Header │ Atoms │ Key Index │ Records │ WAL     │
    └─────────────────────────────────────────────────┘
```

## File Layout

All integers are big-endian unsigned. The file is pre-allocated at creation
time and consists of five contiguous sections:

```
 Offset
 0x00         ┌──────────────────────────────────────┐
              │ Header (32 bytes)                     │
              │   magic: "SHU\0"              4 bytes │
              │   version                     2 bytes │
              │   schema_crc                  4 bytes │
              │   max_key_size                2 bytes │
              │   record_size                 4 bytes │
              │   num_slots                   4 bytes │
              │   wal_capacity                4 bytes │
              │   atom_slot_size              2 bytes │
              │   atom_count                  2 bytes │
              │   atom_table_slots            2 bytes │
              │   reserved                    2 bytes │
 0x20         ├──────────────────────────────────────┤
              │ Atom Table                            │
              │   [0] len:u16 + utf8 data (padded)   │
              │   [1] ...                             │
              │   Pre-allocated: atom_table_slots     │
              │   entries, each atom_slot_size bytes. │
              ├──────────────────────────────────────┤
              │ Key Index                             │
              │   [0] status:u8 + len:u8 +            │
              │       key_data:max_key_size (padded)  │
              │   [1] ...                             │
              │   [num_slots - 1] ...                 │
              │   Dense, contiguous. Scanned on       │
              │   recovery to rebuild key_to_slot.    │
              ├──────────────────────────────────────┤
              │ Record Slots                          │
              │   [0] field_1 .. field_n              │
              │   [1] ...                             │
              │   [num_slots - 1] ...                 │
              │   Each record is record_size bytes.   │
              │   Field order matches schema list.    │
              ├──────────────────────────────────────┤
              │ WAL (Write-Ahead Log)                 │
              │   [0] slot_idx:u32 + field_id:u8 +   │
              │       value:max_hf_size + seq:u64 +   │
              │       crc32:u32                       │
              │   [1] ...                             │
              │   [wal_capacity - 1] ...              │
              │   Fixed-size entries. Append-only     │
              │   until compaction.                   │
              └──────────────────────────────────────┘
```

### Section Rationale

**Key index is separate from record data.** Each key index entry is
`status:u8 + len:u8 + key_data:max_key_size` -- fixed size, with the actual
key length-prefixed and zero-padded. Keys are variable-length (up to 255
bytes) but entries are fixed-width for O(1) slot addressing. The key index is
a dense contiguous section before the mutable record area. This keeps the
record area smaller and more cache-friendly, and makes recovery scanning
faster -- only the key index needs to be read to rebuild the in-memory
`key_to_slot` map.

**WAL is at the end.** High-frequency writes append sequentially to the WAL
section. This avoids seeking back and forth between record slots and the WAL
during mixed workloads.

## Schema

The schema is defined at open time and cannot change. It is validated via a
CRC32 stored in the file header.

```erlang
#{fields := [field_spec()],
  key := {binary, MaxKeySize},
  expected_count => pos_integer()}
```

`MaxKeySize` is the maximum key length in bytes (1--255). Keys can be any
binary from 1 byte up to `MaxKeySize`. They are stored length-prefixed and
zero-padded in the key index so that entry sizes remain fixed for O(1) slot
offset calculation.

Each field spec:

```erlang
#{name := atom(),
  type := shu_type(),
  frequency := low | high}
```

Field list order determines on-disk layout. Field offsets and sizes are
precomputed at open time and stored in `#cfg.field_map` for O(1) lookup by
name.

### File Format Versioning

The file header includes a 16-bit version field. The current version is 1.
On open, the version is validated; files with a different version are
rejected with `{error, {unsupported_version, Version}}`.

This allows for future format evolution:
- **Version 1**: Current format (CRC on WAL entries, atomically truncated WAL).
- **Future versions**: Can introduce new features (e.g., compression, different
  compaction strategy) without risking silent corruption of old files.

To upgrade a file format, use the `migrate/2` API to rewrite data into a new
file with the target format version.

### Frequency

- **`low`** -- written infrequently, durability required. Written directly to
  the record slot via `pwrite` and fsynced immediately. Example: `current_term`,
  `voted_for`.
- **`high`** -- written frequently, durability can be deferred. Written to the
  WAL (no fsync) and cached in a protected ETS table for reads. Example:
  `last_applied`.

## Value Encoding

All values are fixed-size on disk. Each value starts with a 1-byte presence
flag (0 = undefined, 1 = present).

| Type | Encoding | Size |
|------|----------|------|
| `{integer, 64}` | `<<Present:8, Value:64/big>>` | 9 |
| `{atom, _}` | `<<Present:8, AtomIdx:16/big>>` | 3 |
| `{binary, N}` | `<<Present:8, Len:16/big, Data:N/binary>>` | 3 + N |
| `{tuple, Ts}` | `<<Present:8, elements...>>` | 1 + sum |

### Atom Table

Atoms are stored as 16-bit indices into a file-resident atom table. The table
is an array of fixed-size slots at the start of the file (after the header).
Each slot is `atom_slot_size` bytes: a 2-byte UTF-8 length prefix followed by
the atom name padded with zeros.

The atom slot size is inferred from the schema as
`max(MaxBytes for all {atom, MaxBytes} fields) + 2`.

On write, new atoms are appended to the table and the index is written to disk
immediately. The in-memory maps `atom_to_idx` / `idx_to_atom` provide O(1)
lookup in both directions. The table is bounded by `atom_table_slots` (default
256); exceeding it returns `{error, atom_table_full}`.

## WAL Design

The WAL is an append-only buffer of fixed-size entries at the end of the file.
Its purpose is to avoid fsyncing high-frequency field writes on every operation.

### Entry Format

```
slot_index:u32 | field_id:u8 | value:max_hf_field_size | seq:u64 | crc32:u32
```

- `slot_index` is the short-form record ID (not the full key), from the
  in-memory `key_to_slot` map.
- `field_id` is the field's position in the schema list (0-based).
- `value` is the encoded field value, padded to the size of the largest
  high-frequency field.
- `seq` is a monotonically increasing sequence number. A `seq` of 0 indicates
  an empty/unused entry.
- `crc32` is a 32-bit CRC computed over the slot_index, field_id, value, and
  seq fields (everything except the CRC itself).

High-frequency fields are not fsynced, so a crash can leave partial (torn)
writes. On recovery, entries with `seq > 0` and a valid CRC are accepted.
Partial writes that corrupt the CRC are detected and skipped. If a torn write
corrupts only the `seq` field (to 0) but leaves the CRC valid, the entry will
be loaded; if both are corrupted, the CRC mismatch will cause the entry to be
skipped, and the canonical value in the record slot (from the last compaction)
remains valid.

Entry size is fixed and determined by the schema at open time.

### Crash Safety and Torn Writes

WAL entries include a CRC32 checksum over the slot_index, field_id, value, and
seq fields. During recovery, entries are only loaded if their CRC is valid.

Since high-frequency fields are not fsynced, a crash during a WAL write can
result in a "torn write" where the write is partially persisted. The CRC
detects such corruptions:

- If only the value is torn but seq and CRC are intact, the entry is loaded
  with stale data (acceptable since the value will be overwritten by compaction).
- If the CRC itself is corrupted, the entry is skipped and the canonical value
  from the record slot is used instead (from the last successful compaction).
- A crashed truncation leaves the file at the boundary, so recovery sees `eof`
  and treats the WAL as empty (no stale entries from a partial compaction).

### WAL ETS Cache

WAL entries are also stored in a **protected** ETS table (not private) keyed
by `{SlotIndex, FieldId}`. This serves two purposes:

1. Fast reads -- high-frequency field reads check ETS first, avoiding a file
   `pread`.
2. Cross-process compaction -- `do_compact/1` can run in a different process
   and still read the WAL entries from ETS.

The ETS table deduplicates: only the latest value per `{slot, field}` is kept.
`wal_count` tracks the number of unique entries, not total writes.

### WAL Full

When `wal_pos >= wal_capacity`, writes return `{wal_full, State}`. The caller
is expected to trigger compaction. The WAL capacity is
`expected_count * 4` entries.

## Compaction

Compaction flushes WAL values into their canonical record slot locations,
then resets the WAL. It is a three-phase operation designed so the expensive
I/O phase can run in a separate process:

### Phase 1: `prepare_compact(State) -> {Work, State}`

- Snapshots the WAL ETS entries into the `Work` term.
- Sets `compacting = true` in state.
- Further high-frequency writes are buffered in `pending_wal` (in memory)
  rather than written to the file WAL.

### Phase 2: `do_compact(Work) -> ok | {error, Reason}`

- Can run in **any process**.
- Opens the file independently.
- Writes each WAL entry's value to its record slot location via a single
  batched `pwrite`.
- Fsyncs and closes the file.

### Phase 3: `finish_compact(Result, State) -> {ok, State}`

- Truncates the file to end at the WAL region boundary (atomic, crash-safe).
- Fsyncs to durably persist the truncation.
- Clears the WAL ETS table.
- Replays `pending_wal` entries into the now-empty WAL.
- Resets `wal_pos`, `wal_seq`, `wal_count`.
- Sets `compacting = false`.

During compaction, the caller can continue accepting writes. For example, in
the Ra use case, `last_applied` updates arrive as casts and can be
deduplicated by the `gen_batch_server`. Only when a `store_sync` operation
arrives does the caller need the WAL to be writable again.

## Slot Allocation

New files start with `next_free = 0`. Slots are allocated sequentially from
`next_free` until `num_slots` is reached. Deleted slots are added to a
`free_slots` list and reused preferentially on subsequent inserts.

On recovery (reopen), the key index is scanned. Active slots populate
`key_to_slot`. Empty and deleted slots populate `free_slots`.

If all slots are exhausted, writes return `{error, store_full}`.
`expected_count` in the schema should be set generously.

## Recovery

On `open/2` of an existing file:

1. Read and validate the 32-byte header (magic, version, schema CRC).
2. Load the atom table into `atom_to_idx` / `idx_to_atom` maps.
3. Sequentially scan the key index. For each slot, read `status + len + key_data`:
   - `ACTIVE` (1): extract the first `len` bytes of `key_data` as the key,
     add `key -> slot_index` to `key_to_slot`.
   - `DELETED` (2) or `EMPTY` (0): add to `free_slots`.
4. Scan the entire WAL. For each entry with `seq > 0`, keep the highest
   `seq` per `{slot_index, field_id}` in the ETS table.
5. Set `wal_count` from `ets:info(Tab, size)` (actual unique entries).
6. Return `{ok, State}` with the file descriptor open.

## Batch Writes

`write_batch/2` is designed for use behind a `gen_batch_server`. It:

1. Iterates all `{Key, [{Field, Value}]}` operations, allocating slots as
   needed.
2. Classifies each field write as low-frequency (direct pwrite) or
   high-frequency (WAL entry).
3. Issues all low-frequency pwrites in a **single** `file:pwrite(Fd, List)`
   call.
4. Writes all WAL entries.
5. Issues at most **one** `file:sync` if any low-frequency field was in the
   batch.

If the WAL fills up mid-batch, remaining WAL writes are skipped but the batch
continues. The return value is `{wal_full, State}` so the caller knows to
compact.

## Performance Characteristics

- **File I/O**: `file:open` with `[raw, binary]` bypasses the Erlang file
  server. All `pread`/`pwrite`/`sync` calls go directly to the OS via the
  driver.
- **Field lookup**: O(1) via precomputed `#{atom() => #field{}}` map in
  `#cfg{}`.
- **Reads**: High-frequency fields are O(1) ETS lookup. Low-frequency fields
  are a single `pread` at a precomputed offset.
- **Writes**: Low-frequency fields are a single `pwrite` + `fsync`.
  High-frequency fields are a single `pwrite` (WAL append) + ETS insert, no
  fsync.
- **Batch writes**: N keys with M fields total = 1 `pwrite` (low-freq) +
  M WAL `pwrite` calls + 1 `fsync`.
