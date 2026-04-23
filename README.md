# Shu

Shu is a fixed-schema, single-file, high-throughput durable data store for
Erlang/OTP 27+. It is designed to replace DETS in
[Ra](https://github.com/rabbitmq/ra) with something faster, leaner, and more
suitable for the access patterns of Raft metadata.

Named after the Egyptian god of air and light, in the same family as
[Ra](https://github.com/rabbitmq/ra),
[Khepri](https://github.com/rabbitmq/khepri),
[Seshat](https://github.com/rabbitmq/seshat),
[Aten](https://github.com/rabbitmq/aten), and
[Osiris](https://github.com/rabbitmq/osiris).

## Key Properties

- **Fixed schema** -- field names, types, and sizes are declared at open time
  and encoded into the file header. The schema cannot change after creation.
- **Single file** -- all data, metadata, atom table, and WAL live in one file.
- **No mandatory processes** -- shu is a stateful module. The caller owns the
  file descriptor and the state. No gen_server, no ETS ownership surprises.
  Single writer assumed.
- **Frequency-aware writes** -- fields are declared `low` or `high` frequency.
  Low-frequency writes go directly to the record slot and are fsynced
  immediately. High-frequency writes go to an append-only WAL with no fsync,
  and are served from an in-memory ETS cache on read.
- **Batch-friendly** -- `write_batch/2` collects all I/O across keys and
  issues a single `pwrite` + single `fsync`.
- **Two-phase compaction** -- WAL compaction can be offloaded to another
  process without blocking the writer.

## Usage

### Define a Schema

```erlang
Schema = #{fields => [#{name => current_term,
                         type => {integer, 64},
                         frequency => low},
                       #{name => voted_for,
                         type => {tuple, [{atom, 255}, {atom, 255}]},
                         frequency => low},
                       #{name => last_applied,
                         type => {integer, 64},
                         frequency => high}],
           key => {binary, 24},
           expected_count => 50000}.
```

Field list order is the on-disk record layout order. `key` specifies the
maximum key size in bytes (up to 255). Keys can be any binary up to that
length. `expected_count` controls pre-allocation of slots, WAL capacity,
and initial file size.

### Supported Types

| Type | On-disk size | Description |
|------|-------------|-------------|
| `{integer, 64}` | 9 bytes | 64-bit unsigned integer + presence byte |
| `{atom, MaxBytes}` | 3 bytes | 16-bit index into file atom table + presence byte. `MaxBytes` is the max UTF-8 encoded byte length. |
| `{binary, MaxLen}` | 3 + MaxLen bytes | Length-prefixed binary, zero-padded to MaxLen + presence byte |
| `{tuple, [Type]}` | 1 + sum of elements | Presence byte + concatenation of encoded elements |

All fields support `undefined` via a presence flag byte.

### Open / Close

```erlang
{ok, State} = shu:open("/var/data/meta.shu", Schema),
%% ... use State ...
ok = shu:close(State).
```

Opening an existing file validates the schema CRC. A mismatch returns
`{error, schema_mismatch}`.

### Writes

```erlang
%% single field
{ok, State1} = shu:write(State, Key, current_term, 5),

%% multiple fields for one key
{ok, State2} = shu:write(State1, Key,
                          [{current_term, 3},
                           {voted_for, {node1, 'node1@host'}},
                           {last_applied, 99}]),

%% batch across keys (single pwrite + single fsync)
{ok, State3} = shu:write_batch(State2,
                                [{Key1, [{current_term, 1},
                                         {last_applied, 10}]},
                                 {Key2, [{current_term, 2},
                                         {last_applied, 20}]}]).
```

Low-frequency fields are fsynced automatically. High-frequency fields are
written to the WAL without fsync. Use `shu:sync/1` to explicitly fsync the
WAL when durability is needed for high-frequency writes.

Write functions return `{wal_full, State}` when the WAL is exhausted. The
caller should trigger compaction.

### Reads

```erlang
{ok, Value} = shu:read(State, Key, current_term),
{ok, AllFields} = shu:read_all(State, Key).
```

High-frequency fields are read from the in-memory WAL ETS cache. Low-frequency
fields are read directly from the file via `pread`.

### Nulling a Field

Write `undefined` to clear a single field without deleting the record:

```erlang
{ok, State1} = shu:write(State, Key, voted_for, undefined).
```

The field's presence byte is set to 0 on disk. Subsequent reads return
`{ok, undefined}`. Works for both low and high frequency fields.

### Deletes

```erlang
{ok, State1} = shu:delete(State, Key).
```

Marks the slot as deleted (tombstone), purges WAL entries, fsyncs. The slot is
reused on subsequent inserts.

### Compaction

When `write` returns `{wal_full, State}`, the caller should compact:

```erlang
%% Phase 1: snapshot WAL, enter compacting mode
{Work, State1} = shu:prepare_compact(State),

%% Phase 2: can run in ANY process (e.g. spawn a worker)
ok = shu:do_compact(Work),

%% Phase 3: reset WAL, flush pending writes
{ok, State2} = shu:finish_compact(ok, State1).
```

During compaction, the writer can still accept writes. High-frequency writes
are buffered in memory and flushed to the WAL after compaction completes.
This allows the caller to continue deduplicating operations (e.g. `last_applied`
casts) while compaction runs in the background.

### Info

```erlang
#{slot_count := SC,
  num_slots := NS,
  wal_count := WC,
  wal_capacity := Cap,
  wal_usage := Usage,
  atom_count := AC,
  compacting := false} = shu:info(State).
```

### Fold

Iterate over all live keys in the store without exposing the internal state:

```erlang
%% Count all keys
Count = shu:fold(fun(Key, Acc) ->
                   Acc + 1
           end, 0, State),

%% Collect all keys
Keys = shu:fold(fun(Key, Acc) ->
                  [Key | Acc]
           end, [], State),
```

The fold function threads an accumulator through all live keys. Keys are returned in arbitrary order. The internal `#shu{}` state structure is not exposed to the caller.

## Ra Integration Pattern

The primary use case for shu is replacing DETS in `ra_log_meta`. The
recommended schema combines `current_term` and `voted_for` into a single
`term_and_vote` field so that they are updated atomically (one `pwrite` +
one `fsync`). This is required by the Raft protocol -- a crash between
updating the term and clearing the vote could violate safety.

```erlang
Schema = #{fields => [#{name => term_and_vote,
                         type => {tuple, [{integer, 64},
                                          {tuple, [{atom, 255},
                                                   {atom, 255}]}]},
                         frequency => low},
                       #{name => last_applied,
                         type => {integer, 64},
                         frequency => high}],
           key => {binary, 24},
           expected_count => 50000}.
```

Keys are `ra_uid()` binaries which are variable-length (typically a short
prefix plus 12 random characters). The `{binary, 24}` max key size
accommodates all UID lengths.

Usage during an election:

```erlang
%% new term, no vote yet
{ok, S1} = shu:write(S0, Uid, term_and_vote, {5, undefined}),

%% vote cast -- term and vote updated atomically
{ok, S2} = shu:write(S1, Uid, term_and_vote,
                      {5, {ra, 'ra@node1'}}),

%% last_applied updates (high frequency, no fsync)
{ok, S3} = shu:write(S2, Uid, last_applied, 12345).
```

## Build

```bash
# rebar3
rebar3 compile
rebar3 ct

# erlang.mk
make
make ct
```

## License

Dual-licensed under the Apache License 2.0 and the Mozilla Public License 2.0.
See [LICENSE](LICENSE) for details.
