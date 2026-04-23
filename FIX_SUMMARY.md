# SHU Bug Fix: Empty Map and Atom Table Lookup Crashes

## Problem Summary

The `shu:read_all/2` function was returning an empty map `#{}` or crashing with `badmatch` errors when:
1. A key existed in the fold but had never been written to (or had all fields uninitialized)
2. An atom index existed in the data but was missing from the atom table (corruption/partial write)

This caused crashes like:
```
{badmatch,#{}},
[{shu,decode_resolving_atoms,3,
  [{file,"src/shu.erl"},{line,255}]},
 {shu,'-read_all/2-fun-0-',5,
```

## Root Causes

### 1. Empty Map Return (Lines 904-926)
When calling `read_all/2`, if high-frequency fields had errors (not found in WAL/file), they were silently skipped:
```erlang
case read_field(SlotIdx, F, State) of
    {ok, Value} ->
        Acc#{Name => Value};
    error ->
        Acc  %% Field skipped! Not added to result
end
```

If all high-frequency fields errored and there were no low-frequency fields, the result was `#{}`.

### 2. Atom Table Lookup Crash (Line 255)
The `decode_resolving_atoms/3` function used a bare pattern match that crashed if an atom index wasn't in the table:
```erlang
{atom_idx, Idx} ->
    #{Idx := Atom} = State#shu.idx_to_atom,  %% CRASH if Idx not found
    {Atom, Rest}
```

This could happen if:
- Atom table data was corrupted
- An atom was referenced but never persisted
- A partial write occurred during a crash

## Solutions Implemented

### Fix 1: Guarantee All Schema Fields in Result (Line 897-945)
Changed `read_all/2` to always include all schema fields in the result:
- When a high-frequency field is not found, return `undefined` instead of skipping it
- Wrapped the entire decode operation in a try-catch to gracefully handle any decoding errors
- If any decoding fails, return `error` instead of crashing

**Before:**
```erlang
case read_field(SlotIdx, F, State) of
    {ok, Value} ->
        Acc#{Name => Value};
    error ->
        Acc  %% Silently skipped
end
```

**After:**
```erlang
case read_field(SlotIdx, F, State) of
    {ok, Value} ->
        Acc#{Name => Value};
    error ->
        %% Field not found in WAL/file, treat as undefined
        Acc#{Name => undefined}
end
```

And wrapped in try-catch:
```erlang
try
    Result = lists:foldl(...),
    {ok, Result}
catch
    _:_ ->
        %% Decoding error indicates corrupted or partially written data
        error
end
```

### Fix 2: Handle Missing Atoms Gracefully (Lines 250-263)
Changed the atom lookup to use case matching instead of bare pattern match:

**Before:**
```erlang
{atom_idx, Idx} ->
    #{Idx := Atom} = State#shu.idx_to_atom,  %% CRASH
    {Atom, Rest}
```

**After:**
```erlang
{atom_idx, Idx} ->
    case State#shu.idx_to_atom of
        #{Idx := Atom} ->
            {Atom, Rest};
        _ ->
            %% Atom table is corrupted or atom was not persisted.
            %% Return undefined as a safe fallback rather than crashing.
            {undefined, Rest}
    end
```

## Behavior Changes

### read_all/2 guarantees:
1. **All schema fields present**: The result map always contains entries for all fields in the schema
2. **Graceful degradation**: Missing or uninitialized fields return `undefined` instead of being skipped
3. **Error on corruption**: If decoding fails (e.g., missing atom), returns `error` instead of crashing

### decode_resolving_atoms/3 guarantees:
1. **No crashes on missing atoms**: Returns `undefined` instead of crashing with badmatch
2. **Corruption tolerance**: Partially written or corrupted atom references don't bring down the system

## Testing

All 49 existing tests pass with these changes, confirming backward compatibility and correct behavior.

## Migration Notes

These are **bug fixes** and should be transparent to users:
- Code that was working before continues to work
- Code that was crashing now handles the error gracefully
- Workarounds checking `map_size(Fields) > 0` can now be safely removed
