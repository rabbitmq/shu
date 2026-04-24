%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(shu).

-include("shu.hrl").

-export([open/2,
         close/1,
         write/4,
         write/3,
         write_batch/2,
         sync/1,
         read/3,
         read_all/2,
         delete/2,
         prepare_compact/1,
         do_compact/1,
         finish_compact/2,
         info/1,
         fold/3,
         fold/4,
         migrate/2]).

-ifdef(TEST).
-export([encode_value/2,
         decode_value/2]).
-endif.

-opaque state() :: #shu{}.

-export_type([state/0,
              schema/0,
              field_spec/0,
              shu_type/0,
              compact_work/0,
              compact_result/0]).

-compile({inline, [key_index_pos/2,
                   record_pos/2,
                   field_pos/3,
                   wal_entry_pos/2,
                   lookup_field/2]}).

%%% ============================================================
%%% Schema validation and field computation
%%% ============================================================

-spec validate_schema(schema()) -> {ok, #cfg{}} | {error, term()}.
validate_schema(#{fields := Fields, key := {binary, MaxKeySize}} = Schema)
  when is_list(Fields), length(Fields) > 0,
       is_integer(MaxKeySize), MaxKeySize > 0, MaxKeySize =< 255 ->
    ExpectedCount = maps:get(expected_count, Schema, ?DEFAULT_EXPECTED_COUNT),
    try
        {FieldRecs, RecordSize} = compute_fields(Fields),
        HighFreq = [F || #field{frequency = high} = F <- FieldRecs],
        LowFreq = [F || #field{frequency = low} = F <- FieldRecs],
        FieldMap = maps:from_list(
                     [{F#field.name, F} || F <- FieldRecs]),
        AtomSlotSize = compute_atom_slot_size(Fields),
        AtomTableSlots = ?DEFAULT_ATOM_TABLE_SLOTS,
        WalEntrySize = compute_wal_entry_size(HighFreq),
        WalCapacity = ExpectedCount * ?WAL_CAPACITY_MULTIPLIER,
        SchemaCrc = compute_schema_crc(Fields, MaxKeySize),
        AtomTableOffset = ?HEADER_SIZE,
        KeyIndexOffset = AtomTableOffset + AtomTableSlots * AtomSlotSize,
        KeyIndexEntrySize = 1 + 1 + MaxKeySize,
        RecordOffset = KeyIndexOffset + ExpectedCount * KeyIndexEntrySize,
        WalOffset = RecordOffset + ExpectedCount * RecordSize,
        Cfg = #cfg{filename = undefined,
                   schema_crc = SchemaCrc,
                   max_key_size = MaxKeySize,
                   record_size = RecordSize,
                   num_slots = ExpectedCount,
                   fields = FieldRecs,
                   field_map = FieldMap,
                   high_freq_fields = HighFreq,
                   low_freq_fields = LowFreq,
                   atom_slot_size = AtomSlotSize,
                   atom_table_slots = AtomTableSlots,
                   wal_entry_size = WalEntrySize,
                   wal_capacity = WalCapacity,
                   atom_table_offset = AtomTableOffset,
                   key_index_offset = KeyIndexOffset,
                   record_offset = RecordOffset,
                   wal_offset = WalOffset},
        {ok, Cfg}
    catch
        throw:Reason -> {error, Reason}
    end;
validate_schema(_) ->
    {error, invalid_schema}.

-spec compute_fields([field_spec()]) -> {[#field{}], pos_integer()}.
compute_fields(Fields) ->
    {Recs, _Id, Offset} =
        lists:foldl(
          fun(#{name := Name, type := Type, frequency := Freq}, {Acc, Id, Off}) ->
                  Size = type_size(Type),
                  F = #field{name = Name,
                             id = Id,
                             type = Type,
                             frequency = Freq,
                             offset = Off,
                             size = Size},
                  {[F | Acc], Id + 1, Off + Size};
             (_, _) ->
                  throw(invalid_field_spec)
          end, {[], 0, 0}, Fields),
    {lists:reverse(Recs), Offset}.

-spec type_size(shu_type()) -> pos_integer().
type_size({integer, 64}) ->
    1 + 8;
type_size({atom, _MaxBytes}) ->
    1 + 2;
type_size({binary, MaxLen}) ->
    1 + 2 + MaxLen;
type_size({tuple, Types}) ->
    1 + lists:sum([type_size(T) || T <- Types]);
type_size(_) ->
    throw(invalid_type).

-spec compute_atom_slot_size([field_spec()]) -> pos_integer().
compute_atom_slot_size(Fields) ->
    MaxBytes = lists:foldl(
                 fun(#{type := Type}, Acc) ->
                         max(Acc, max_atom_bytes(Type));
                    (_, Acc) -> Acc
                 end, 0, Fields),
    case MaxBytes of
        0 -> 32;
        N -> N + 2
    end.

max_atom_bytes({atom, MaxBytes}) -> MaxBytes;
max_atom_bytes({tuple, Types}) ->
    lists:max([0 | [max_atom_bytes(T) || T <- Types]]);
max_atom_bytes(_) -> 0.

-spec compute_wal_entry_size([#field{}]) -> pos_integer().
compute_wal_entry_size([]) ->
    4 + 1 + 0 + 8 + 4;
compute_wal_entry_size(HighFreqFields) ->
    MaxFieldSize = lists:max([S || #field{size = S} <- HighFreqFields]),
    4 + 1 + MaxFieldSize + 8 + 4.

-spec compute_schema_crc([field_spec()], pos_integer()) -> non_neg_integer().
compute_schema_crc(Fields, KeySize) ->
    erlang:crc32(term_to_binary({Fields, KeySize})).

%%% ============================================================
%%% Value encoding / decoding
%%% ============================================================

-spec encode_value(shu_type(), term()) -> binary().
encode_value({integer, 64}, undefined) ->
    <<0:8, 0:64>>;
encode_value({integer, 64}, Value) when is_integer(Value), Value >= 0 ->
    <<1:8, Value:64/unsigned-big>>;
encode_value({atom, _MaxBytes}, undefined) ->
    <<0:8, 0:16>>;
encode_value({atom, _MaxBytes}, {atom_idx, Idx}) ->
    <<1:8, Idx:16/unsigned-big>>;
encode_value({binary, MaxLen}, undefined) ->
    <<0:8, 0:16, 0:(MaxLen * 8)>>;
encode_value({binary, MaxLen}, Value) when is_binary(Value) ->
    Len = byte_size(Value),
    PadSize = MaxLen - Len,
    <<1:8, Len:16/unsigned-big, Value/binary, 0:(PadSize * 8)>>;
encode_value({tuple, Types}, undefined) ->
    InnerSize = lists:sum([type_size(T) || T <- Types]),
    <<0:8, 0:(InnerSize * 8)>>;
encode_value({tuple, Types}, Value) when is_tuple(Value) ->
    Elements = tuple_to_list(Value),
    Inner = list_to_binary(lists:zipwith(fun encode_value/2, Types, Elements)),
    <<1:8, Inner/binary>>.

-spec decode_value(shu_type(), binary()) -> {term(), binary()}.
decode_value({integer, 64}, <<0:8, _:64, Rest/binary>>) ->
    {undefined, Rest};
decode_value({integer, 64}, <<1:8, Value:64/unsigned-big, Rest/binary>>) ->
    {Value, Rest};
decode_value({atom, _MaxBytes}, <<0:8, _:16, Rest/binary>>) ->
    {undefined, Rest};
decode_value({atom, _MaxBytes}, <<1:8, Idx:16/unsigned-big, Rest/binary>>) ->
    {{atom_idx, Idx}, Rest};
decode_value({binary, MaxLen}, Bin) ->
    TotalSize = 1 + 2 + MaxLen,
    <<Chunk:TotalSize/binary, Rest/binary>> = Bin,
    case Chunk of
        <<0:8, _/binary>> ->
            {undefined, Rest};
        <<1:8, Len:16/unsigned-big, Data/binary>> ->
            <<Value:Len/binary, _/binary>> = Data,
            {Value, Rest}
    end;
decode_value({tuple, Types}, Bin) ->
    InnerSize = lists:sum([type_size(T) || T <- Types]),
    <<Present:8, Inner:InnerSize/binary, Rest/binary>> = Bin,
    case Present of
        0 ->
            {undefined, Rest};
        1 ->
            {Elements, <<>>} =
                lists:foldl(fun(T, {Acc, B}) ->
                                    {V, B2} = decode_value(T, B),
                                    {[V | Acc], B2}
                            end, {[], Inner}, Types),
            {list_to_tuple(lists:reverse(Elements)), Rest}
    end.

-spec encode_field_value(#field{}, term(), #shu{}) -> {binary(), #shu{}}.
encode_field_value(#field{type = Type}, Value, State) ->
    encode_resolving_atoms(Type, Value, State).

encode_resolving_atoms({atom, MaxBytes}, undefined, State) ->
    {encode_value({atom, MaxBytes}, undefined), State};
encode_resolving_atoms({atom, MaxBytes}, Atom, State) when is_atom(Atom) ->
    case ensure_atom(Atom, State) of
        {ok, Idx, State1} ->
            {encode_value({atom, MaxBytes}, {atom_idx, Idx}), State1};
        {error, _} = Err ->
            throw(Err)
    end;
encode_resolving_atoms({tuple, Types}, undefined, State) ->
    {encode_value({tuple, Types}, undefined), State};
encode_resolving_atoms({tuple, Types}, Value, State)
  when is_tuple(Value) ->
    Elements = tuple_to_list(Value),
    {BinsRev, State1} =
        lists:foldl(fun({T, V}, {Acc, S}) ->
                            {B, S1} = encode_resolving_atoms(T, V, S),
                            {[B | Acc], S1}
                    end, {[], State}, lists:zip(Types, Elements)),
    %% Build binary directly from reversed list to avoid list_to_binary
    %% overhead of reversing again
    Inner = iolist_to_binary(lists:reverse(BinsRev)),
    {<<1:8, Inner/binary>>, State1};
encode_resolving_atoms(Type, Value, State) ->
    {encode_value(Type, Value), State}.

-spec decode_field_value(#field{}, binary(), #shu{}) -> {term(), binary()}.
decode_field_value(#field{type = Type}, Bin, State) ->
    decode_resolving_atoms(Type, Bin, State).

decode_resolving_atoms({atom, MaxBytes}, Bin, State) ->
    {Val, Rest} = decode_value({atom, MaxBytes}, Bin),
    case Val of
        undefined -> {undefined, Rest};
        {atom_idx, Idx} ->
            case State#shu.idx_to_atom of
                #{Idx := Atom} ->
                    {Atom, Rest};
                _ ->
                    %% Atom table is corrupted or atom was not persisted.
                    %% Return undefined as a safe fallback rather than crashing.
                    {undefined, Rest}
            end
    end;
decode_resolving_atoms({tuple, Types}, Bin, State) ->
    InnerSize = lists:sum([type_size(T) || T <- Types]),
    <<Present:8, Inner:InnerSize/binary, Rest/binary>> = Bin,
    case Present of
        0 ->
            {undefined, Rest};
        1 ->
            {Elements, <<>>} =
                lists:foldl(fun(T, {Acc, B}) ->
                                    {V, B2} = decode_resolving_atoms(T, B, State),
                                    {[V | Acc], B2}
                            end, {[], Inner}, Types),
            {list_to_tuple(lists:reverse(Elements)), Rest}
    end;
decode_resolving_atoms(Type, Bin, _State) ->
    decode_value(Type, Bin).

%%% ============================================================
%%% Atom table management
%%% ============================================================

-spec ensure_atom(atom(), #shu{}) ->
    {ok, non_neg_integer(), #shu{}} | {error, atom_table_full}.
ensure_atom(Atom, #shu{atom_to_idx = A2I} = State) ->
    case A2I of
        #{Atom := Idx} ->
            {ok, Idx, State};
        _ ->
            add_atom(Atom, State)
    end.

-spec add_atom(atom(), #shu{}) ->
    {ok, non_neg_integer(), #shu{}} | {error, atom_table_full}.
add_atom(Atom, #shu{cfg = #cfg{atom_slot_size = SlotSize,
                                atom_table_offset = AtomOff,
                                atom_table_slots = AtomTableSlots},
                     fd = Fd,
                     atom_to_idx = A2I,
                     idx_to_atom = I2A,
                     atom_count = Count} = State) ->
    case Count >= AtomTableSlots of
        true ->
            {error, atom_table_full};
        false ->
            Idx = Count,
            Bin = atom_to_binary(Atom, utf8),
            Len = byte_size(Bin),
            DataSize = SlotSize - 2,
            PadSize = DataSize - Len,
            Entry = <<Len:16/unsigned-big, Bin/binary, 0:(PadSize * 8)>>,
            Pos = AtomOff + Idx * SlotSize,
            ok = prim_file:pwrite(Fd, Pos, Entry),
            ok = prim_file:sync(Fd),
            {ok, Idx, State#shu{atom_to_idx = A2I#{Atom => Idx},
                                idx_to_atom = I2A#{Idx => Atom},
                                atom_count = Count + 1}}
    end.

-spec load_atom_table(file:io_device(), #cfg{}, non_neg_integer()) ->
    {#{atom() => non_neg_integer()},
     #{non_neg_integer() => atom()},
     non_neg_integer()}.
load_atom_table(_Fd, _Cfg, 0) ->
    {#{}, #{}, 0};
load_atom_table(Fd,
                #cfg{atom_slot_size = SlotSize,
                     atom_table_offset = AtomOff,
                     atom_table_slots = AtomTableSlots},
                HeaderAtomCount) ->
    %% Read all atom table slots to find the actual count
    %% (in case header count is stale due to crash during add_atom)
    TotalSize = AtomTableSlots * SlotSize,
    {ok, Bin} = file:pread(Fd, AtomOff, TotalSize),
    %% Find first empty slot (Len=0) or use HeaderAtomCount as fallback
    ActualCount = find_atom_table_boundary(Bin, SlotSize, 0,
                                           HeaderAtomCount,
                                           AtomTableSlots),
    %% Now parse up to the actual count
    ParseSize = ActualCount * SlotSize,
    <<ParseBin:ParseSize/binary, _/binary>> = Bin,
    parse_atom_table(ParseBin, SlotSize, 0, ActualCount, #{}, #{}).

parse_atom_table(_Bin, _SlotSize, Idx, AtomCount, A2I, I2A)
  when Idx >= AtomCount ->
    {A2I, I2A, AtomCount};
parse_atom_table(Bin, SlotSize, Idx, AtomCount, A2I, I2A) ->
    <<Entry:SlotSize/binary, Rest/binary>> = Bin,
    <<Len:16/unsigned-big, Data/binary>> = Entry,
    <<AtomBin:Len/binary, _/binary>> = Data,
    Atom = binary_to_atom(AtomBin, utf8),
    parse_atom_table(Rest, SlotSize, Idx + 1, AtomCount,
                     A2I#{Atom => Idx}, I2A#{Idx => Atom}).

find_atom_table_boundary(_Bin, _SlotSize, Idx, MaxIdx, TableSlots)
  when Idx >= MaxIdx orelse Idx >= TableSlots ->
    %% Reached header count or table limit without finding empty slot
    Idx;
find_atom_table_boundary(Bin, SlotSize, Idx, MaxIdx, TableSlots) ->
    %% Check if this slot is empty (first 2 bytes = 0 means Len=0)
    <<Entry:SlotSize/binary, Rest/binary>> = Bin,
    <<Len:16/unsigned-big, _/binary>> = Entry,
    case Len of
        0 ->
            %% Found first empty slot, actual count is Idx
            Idx;
        _ ->
            %% Slot occupied, continue scanning
            find_atom_table_boundary(Rest, SlotSize, Idx + 1, MaxIdx,
                                     TableSlots)
    end.

%%% ============================================================
%%% File addressing helpers
%%% ============================================================

key_index_pos(#cfg{key_index_offset = Off, max_key_size = MKS}, SlotIdx) ->
    Off + SlotIdx * (1 + 1 + MKS).

record_pos(#cfg{record_offset = Off, record_size = RS}, SlotIdx) ->
    Off + SlotIdx * RS.

field_pos(Cfg, SlotIdx, #field{offset = FldOff}) ->
    record_pos(Cfg, SlotIdx) + FldOff.

wal_entry_pos(#cfg{wal_offset = Off, wal_entry_size = ES}, WalIdx) ->
    Off + WalIdx * ES.

lookup_field(Name, #cfg{field_map = FM}) ->
    case FM of
        #{Name := F} -> {ok, F};
        _ -> error
    end.

%%% ============================================================
%%% File header
%%% ============================================================

write_header(Fd, #cfg{schema_crc = Crc,
                       max_key_size = MaxKeySize,
                       record_size = RecordSize,
                       num_slots = NumSlots,
                       wal_capacity = WalCap,
                       atom_slot_size = AtomSlotSize,
                       atom_table_slots = AtomTableSlots},
             AtomCount) ->
    Header = <<?MAGIC,
               ?VERSION:16/unsigned-big,
               Crc:32/unsigned-big,
               MaxKeySize:16/unsigned-big,
               RecordSize:32/unsigned-big,
               NumSlots:32/unsigned-big,
               WalCap:32/unsigned-big,
               AtomSlotSize:16/unsigned-big,
               AtomCount:16/unsigned-big,
               AtomTableSlots:16/unsigned-big,
               0:16>>,
    ok = file:pwrite(Fd, 0, Header).

read_header(Fd) ->
    case file:pread(Fd, 0, ?HEADER_SIZE) of
        {ok, <<?MAGIC,
               Version:16/unsigned-big,
               Crc:32/unsigned-big,
               MaxKeySize:16/unsigned-big,
               RecordSize:32/unsigned-big,
               NumSlots:32/unsigned-big,
               WalCap:32/unsigned-big,
               AtomSlotSize:16/unsigned-big,
               AtomCount:16/unsigned-big,
               AtomTableSlots:16/unsigned-big,
               _Reserved:16>>} ->
            {ok, #{version => Version,
                   schema_crc => Crc,
                   max_key_size => MaxKeySize,
                   record_size => RecordSize,
                   num_slots => NumSlots,
                   wal_capacity => WalCap,
                   atom_slot_size => AtomSlotSize,
                   atom_count => AtomCount,
                   atom_table_slots => AtomTableSlots}};
        {ok, _} ->
            {error, invalid_header};
        eof ->
            {error, empty_file};
        {error, _} = Err ->
            Err
    end.

%%% ============================================================
%%% open / close
%%% ============================================================

-spec open(file:filename_all(), schema()) ->
    {ok, state()} | {error, term()}.
open(Filename, Schema) ->
    case validate_schema(Schema) of
        {ok, Cfg0} ->
            Cfg = Cfg0#cfg{filename = Filename},
            case prim_file:read_file_info(Filename) of
                {ok, _} ->
                    open_existing(Filename, Cfg);
                {error, enoent} ->
                    create_new(Filename, Cfg)
            end;
        {error, _} = Err ->
            Err
    end.

create_new(Filename, #cfg{num_slots = NumSlots,
                           max_key_size = MaxKeySize,
                           record_size = RecordSize,
                           wal_capacity = WalCap,
                           wal_entry_size = WalEntrySize,
                           atom_table_slots = AtomTableSlots,
                           atom_slot_size = AtomSlotSize} = Cfg) ->
    {ok, Fd} = file:open(Filename, [read, write, raw, binary]),
    ok = write_header(Fd, Cfg, 0),
    KeyIndexSize = NumSlots * (1 + 1 + MaxKeySize),
    RecordAreaSize = NumSlots * RecordSize,
    WalSize = WalCap * WalEntrySize,
    TotalSize = ?HEADER_SIZE +
                AtomTableSlots * AtomSlotSize +
                KeyIndexSize + RecordAreaSize + WalSize,
    ok = file:pwrite(Fd, TotalSize - 1, <<0>>),
    ok = file:sync(Fd),
    Tab = ets:new(shu_wal, [set, protected, {keypos, 1}]),
    {ok, #shu{cfg = Cfg,
              fd = Fd,
              next_free = 0,
              free_slots = [],
              wal_tab = Tab}}.

open_existing(Filename, #cfg{schema_crc = ExpectedCrc} = Cfg0) ->
    {ok, Fd} = file:open(Filename, [read, write, raw, binary]),
    case read_header(Fd) of
        {ok, #{version := ?VERSION,
               schema_crc := ExpectedCrc,
               num_slots := NumSlots,
               atom_slot_size := AtomSlotSize,
               atom_count := AtomCount,
               atom_table_slots := AtomTableSlots,
               wal_capacity := WalCap}} ->
            AtomTableOffset = ?HEADER_SIZE,
            KeyIndexOffset = AtomTableOffset + AtomTableSlots * AtomSlotSize,
            RecordOffset = KeyIndexOffset +
                           NumSlots * (1 + 1 + Cfg0#cfg.max_key_size),
            WalOffset = RecordOffset + NumSlots * Cfg0#cfg.record_size,
            Cfg = Cfg0#cfg{num_slots = NumSlots,
                           atom_slot_size = AtomSlotSize,
                           atom_table_slots = AtomTableSlots,
                           wal_capacity = WalCap,
                           atom_table_offset = AtomTableOffset,
                           key_index_offset = KeyIndexOffset,
                           record_offset = RecordOffset,
                           wal_offset = WalOffset},
            {A2I, I2A, AC} = load_atom_table(Fd, Cfg, AtomCount),
            Tab = ets:new(shu_wal, [set, protected, {keypos, 1}]),
            State0 = #shu{cfg = Cfg,
                          fd = Fd,
                          atom_to_idx = A2I,
                          idx_to_atom = I2A,
                          atom_count = AC,
                          next_free = NumSlots,
                          wal_tab = Tab},
            State1 = recover_key_index(Fd, Cfg, State0),
            State2 = recover_wal(Fd, Cfg, State1),
            {ok, State2};
        {ok, #{version := V}} when V =/= ?VERSION ->
            _ = file:close(Fd),
            {error, {unsupported_version, V}};
        {ok, #{schema_crc := _Other}} ->
            _ = file:close(Fd),
            {error, schema_mismatch};
        {error, _} = Err ->
            _ = file:close(Fd),
            Err
    end.

-spec close(state()) -> ok | {error, term()}.
close(#shu{compacting = true} = _State) ->
    %% Cannot close while compaction is in progress. The file layout is
    %% inconsistent and potentially being written to by do_compact/1.
    %% Complete the compaction (finish_compact) before closing.
    {error, compaction_in_progress};
close(State0) ->
    #shu{fd = Fd, wal_tab = Tab, cfg = Cfg,
         atom_count = AtomCount} = flush_pending_wal(State0),
    ok = write_header(Fd, Cfg, AtomCount),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    true = ets:delete(Tab),
    ok.

%%% ============================================================
%%% Recovery
%%% ============================================================

recover_key_index(Fd, #cfg{max_key_size = MaxKeySize,
                           num_slots = NumSlots} = Cfg,
                  State0) ->
    EntrySize = 1 + 1 + MaxKeySize,
    TotalSize = NumSlots * EntrySize,
    if TotalSize > 0 ->
            StartPos = key_index_pos(Cfg, 0),
            {ok, Bin} = file:pread(Fd, StartPos, TotalSize),
            scan_key_index(Bin, MaxKeySize, 0, NumSlots, State0);
       true ->
            State0
    end.

scan_key_index(_Bin, _MaxKeySize, Idx, NumSlots, State)
  when Idx >= NumSlots ->
    State;
scan_key_index(Bin, MaxKeySize, Idx, NumSlots,
               #shu{key_to_slot = K2S, free_slots = Free,
                    slot_count = SC} = State) ->
    <<Status:8, KeyLen:8, KeyData:MaxKeySize/binary, Rest/binary>> = Bin,
    case Status of
        ?SLOT_ACTIVE ->
            <<Key:KeyLen/binary, _/binary>> = KeyData,
            scan_key_index(Rest, MaxKeySize, Idx + 1, NumSlots,
                           State#shu{key_to_slot = K2S#{Key => Idx},
                                     slot_count = SC + 1});
        _ ->
            scan_key_index(Rest, MaxKeySize, Idx + 1, NumSlots,
                           State#shu{free_slots = [Idx | Free]})
    end.

recover_wal(Fd, #cfg{wal_capacity = WalCap, wal_entry_size = EntrySize,
                      wal_offset = WalOff},
            State0) ->
    TotalSize = WalCap * EntrySize,
    if TotalSize > 0 ->
            case file:pread(Fd, WalOff, TotalSize) of
                {ok, Bin} ->
                    State1 = scan_wal(Bin, EntrySize, 0, WalCap, State0),
                    %% wal_count must reflect actual unique ETS entries
                    WalCount = ets:info(State1#shu.wal_tab, size),
                    State1#shu{wal_count = WalCount};
                eof ->
                    %% File was truncated (e.g., after successful compaction).
                    %% WAL region does not exist; no entries to recover.
                    State0;
                {error, _} ->
                    %% File read error; treat as no WAL to recover.
                    State0
            end;
       true ->
            State0
    end.


scan_wal(<<>>, _EntrySize, _Idx, _WalCap, State) ->
    State;
scan_wal(Bin, EntrySize, Idx, WalCap, State) when Idx < WalCap ->
    <<Entry:EntrySize/binary, Rest/binary>> = Bin,
    <<SlotIdx:32/unsigned-big, FieldId:8, ValueAndMeta/binary>> = Entry,
    ValueSize = EntrySize - 4 - 1 - 8 - 4,
    <<Value:ValueSize/binary, Seq:64/unsigned-big, StoredCrc:32/unsigned-big>> = ValueAndMeta,
    %% Verify CRC: recompute over slot_idx, field_id, value, and seq
    CrcData = <<SlotIdx:32/unsigned-big, FieldId:8,
                Value/binary, Seq:64/unsigned-big>>,
    ComputedCrc = erlang:crc32(CrcData),
    State1 = case {Seq, ComputedCrc} of
                 {0, _} ->
                     %% Empty entry
                     State;
                 {_, ComputedCrc} when ComputedCrc =:= StoredCrc ->
                     %% CRC matches; this entry is valid
                     %% Skip WAL entries for slots that are marked
                     %% as deleted/free (e.g., freed slots from
                     %% delete operations)
                     case lists:member(SlotIdx, State#shu.free_slots) of
                         true ->
                             %% Slot is free, skip orphaned WAL entry
                             State;
                         false ->
                             #shu{wal_tab = Tab, wal_seq = MaxSeq} = State,
                             Key = {SlotIdx, FieldId},
                             ShouldInsert =
                                 case ets:lookup(Tab, Key) of
                                     [{_, _, ExistingSeq}]
                                       when ExistingSeq >= Seq ->
                                         false;
                                     _ ->
                                         true
                                 end,
                             case ShouldInsert of
                                 true ->
                                     true = ets:insert(Tab, {Key, Value, Seq}),
                                     State#shu{wal_seq = max(MaxSeq, Seq),
                                               wal_pos = max(State#shu.wal_pos,
                                                             Idx + 1)};
                                 false ->
                                     State
                             end
                     end;
                 {_, _} ->
                     %% CRC mismatch; torn write detected. Skip this entry.
                     State
             end,
    scan_wal(Rest, EntrySize, Idx + 1, WalCap, State1);
scan_wal(_, _EntrySize, _Idx, _WalCap, State) ->
    State.

%%% ============================================================
%%% Writes
%%% ============================================================

-spec write(state(), Key :: binary(), atom(), term()) ->
    {ok, state()} | {wal_full, state()} | {error, term()}.
write(State, Key, FieldName, Value) ->
    write(State, Key, [{FieldName, Value}]).

-spec write(state(), Key :: binary(), [{atom(), term()}]) ->
    {ok, state()} | {wal_full, state()} | {error, term()}.
write(#shu{cfg = Cfg} = State0, Key, FieldValues) ->
    case ensure_slot(Key, State0) of
        {ok, SlotIdx, State1} ->
            do_write_fields(Cfg, SlotIdx, FieldValues, State1);
        {error, _} = Err ->
            Err
    end.

-spec write_batch(state(), [{Key :: binary(), [{atom(), term()}]}]) ->
    {ok, state()} | {wal_full, state()} | {error, term()}.
write_batch(State, Ops) ->
    do_write_batch(Ops, State, [], [], false, false).

%% Collect all pwrite operations across all keys, then issue them
%% together with at most one fsync.
do_write_batch([], State, LowAcc, WalAcc, NeedSync, WalFull) ->
    Fd = State#shu.fd,
    State1 = case LowAcc of
                 [] -> State;
                 _ ->
                     %% LowAcc is built in reverse, flatten lists and reverse to get correct order
                     AllLow = lists:append(lists:reverse(LowAcc)),
                     ok = file:pwrite(Fd, AllLow),
                     State
             end,
    %% issue all WAL writes - WalAcc is also built in reverse
    AllWal = lists:append(lists:reverse(WalAcc)),
    {State2, WalFull2} = flush_wal_acc(AllWal, State1, WalFull),
    case NeedSync of
        true -> ok = file:sync(Fd);
        false -> ok
    end,
    case WalFull2 of
        true -> {wal_full, State2};
        false -> {ok, State2}
    end;
do_write_batch([{Key, FieldValues} | Rest], State0, LowAcc, WalAcc,
               NeedSync, WalFull) ->
    case ensure_slot(Key, State0) of
        {ok, SlotIdx, State1} ->
            Cfg = State1#shu.cfg,
            {NewLow, NewWal, State2, HasLow} =
                classify_fields(Cfg, SlotIdx, FieldValues, State1),
            %% Build accumulators as lists of lists, prepending (cons) instead of append
            %% O(1) cons instead of O(N) append - will flatten at the end
            do_write_batch(Rest, State2,
                           [NewLow | LowAcc],
                           [NewWal | WalAcc],
                           NeedSync orelse HasLow,
                           WalFull);
        {error, _} = Err ->
            Err
    end.

classify_fields(Cfg, SlotIdx, FieldValues, State) ->
    lists:foldl(
      fun({Name, Value}, {LW, WW, S, HasLow}) ->
              case lookup_field(Name, Cfg) of
                  {ok, #field{frequency = low} = F} ->
                      {Encoded, S1} = encode_field_value(F, Value, S),
                      Pos = field_pos(Cfg, SlotIdx, F),
                      {[{Pos, Encoded} | LW], WW, S1, true};
                  {ok, #field{frequency = high} = F} ->
                      {Encoded, S1} = encode_field_value(F, Value, S),
                      {LW, [{SlotIdx, F, Encoded} | WW], S1, HasLow};
                  error ->
                      throw({unknown_field, Name})
              end
      end, {[], [], State, false}, FieldValues).

flush_wal_acc(WalWrites, State, WalFull) ->
    {State1, WalFull1, PWrites} =
        flush_wal_acc_loop(WalWrites, State, WalFull, []),
    case PWrites of
        [] -> ok;
        _ -> ok = file:pwrite(State1#shu.fd, lists:reverse(PWrites))
    end,
    {State1, WalFull1}.

flush_wal_acc_loop([], State, WalFull, Acc) ->
    {State, WalFull, Acc};
flush_wal_acc_loop([{SlotIdx, Field, Encoded} | Rest], State, WalFull, Acc) ->
    case prepare_single_wal_entry(SlotIdx, Field, Encoded, State) of
        {ok, State1, undefined} ->
            flush_wal_acc_loop(Rest, State1, WalFull, Acc);
        {ok, State1, PWrite} ->
            flush_wal_acc_loop(Rest, State1, WalFull, [PWrite | Acc]);
        {wal_full, State1} ->
            flush_wal_acc_loop(Rest, State1, true, Acc)
    end.

ensure_slot(Key, #shu{key_to_slot = K2S} = State) ->
    case K2S of
        #{Key := SlotIdx} ->
            {ok, SlotIdx, State};
        _ ->
            allocate_slot(Key, State)
    end.

allocate_slot(Key, #shu{cfg = Cfg, fd = Fd,
                         free_slots = Free,
                         next_free = NextFree,
                         key_to_slot = K2S,
                         slot_count = SC} = State) ->
    #cfg{max_key_size = MaxKeySize, num_slots = NumSlots} = Cfg,
    KeyLen = byte_size(Key),
    case KeyLen > 0 andalso KeyLen =< MaxKeySize of
        false ->
            {error, {invalid_key_size, KeyLen, MaxKeySize}};
        true ->
            case Free of
                [SlotIdx | Rest] ->
                    write_key_index_entry(Fd, Cfg, SlotIdx, Key),
                    {ok, SlotIdx,
                     State#shu{free_slots = Rest,
                               key_to_slot = K2S#{Key => SlotIdx},
                               slot_count = SC + 1}};
                [] when NextFree < NumSlots ->
                    write_key_index_entry(Fd, Cfg, NextFree, Key),
                    {ok, NextFree,
                     State#shu{next_free = NextFree + 1,
                               key_to_slot = K2S#{Key => NextFree},
                               slot_count = SC + 1}};
                [] ->
                    {error, store_full}
            end
    end.

write_key_index_entry(Fd, #cfg{max_key_size = MaxKeySize} = Cfg,
                      SlotIdx, Key) ->
    Pos = key_index_pos(Cfg, SlotIdx),
    KeyLen = byte_size(Key),
    PadSize = MaxKeySize - KeyLen,
    ok = file:pwrite(Fd, Pos, <<?SLOT_ACTIVE:8, KeyLen:8,
                                 Key/binary, 0:(PadSize * 8)>>).

do_write_fields(Cfg, SlotIdx, FieldValues, State) ->
    {LowWrites, WalWrites, State1, NeedSync} =
        classify_fields(Cfg, SlotIdx, FieldValues, State),
    Fd = State1#shu.fd,
    case LowWrites of
        [] -> ok;
        [{Pos, Data}] ->
            ok = file:pwrite(Fd, Pos, Data);
        _ ->
            ok = file:pwrite(Fd, LowWrites)
    end,
    {State2, WalFull} = flush_wal_acc(WalWrites, State1, false),
    case NeedSync of
        true -> ok = file:sync(Fd);
        false -> ok
    end,
    case WalFull of
        true -> {wal_full, State2};
        false -> {ok, State2}
    end.

prepare_single_wal_entry(SlotIdx, #field{id = FieldId},
                        Encoded,
                        #shu{cfg = Cfg, fd = _Fd,
                             wal_pos = WalPos, wal_seq = WalSeq,
                             wal_count = WalCount, wal_tab = Tab,
                             compacting = Compacting,
                             pending_wal = Pending} = State) ->
    #cfg{wal_capacity = WalCap, wal_entry_size = EntrySize} = Cfg,
    case WalPos >= WalCap andalso not Compacting of
        true ->
            {wal_full, State};
        false ->
            Seq = WalSeq + 1,
            %% Entry format: slot_idx:u32 | field_id:u8 | value:N | seq:u64 | crc:u32
            %% CRC is computed over slot_idx, field_id, value, and seq.
            ValueSize = EntrySize - 4 - 1 - 8 - 4,
            PadSize = ValueSize - byte_size(Encoded),
            PaddedValue = <<Encoded/binary, 0:(PadSize * 8)>>,
            CrcData = <<SlotIdx:32/unsigned-big, FieldId:8,
                        PaddedValue/binary, Seq:64/unsigned-big>>,
            Crc = erlang:crc32(CrcData),
            Entry = <<CrcData/binary, Crc:32/unsigned-big>>,
            EtsKey = {SlotIdx, FieldId},
            IsNew = case ets:lookup(Tab, EtsKey) of
                        [] -> true;
                        _ -> false
                    end,
            true = ets:insert(Tab, {EtsKey, PaddedValue, Seq}),
            NewWalCount = case IsNew of
                              true -> WalCount + 1;
                              false -> WalCount
                          end,
            case Compacting of
                true ->
                    {ok, State#shu{wal_seq = Seq,
                                   wal_count = NewWalCount,
                                   pending_wal = [Entry | Pending]}, undefined};
                false ->
                    ActualPos = WalPos rem WalCap,
                    FilePos = wal_entry_pos(Cfg, ActualPos),
                    {ok, State#shu{wal_pos = WalPos + 1,
                                   wal_seq = Seq,
                                   wal_count = NewWalCount}, {FilePos, Entry}}
            end
    end.

%%% ============================================================
%%% Reads
%%% ============================================================

-spec read(state(), Key :: binary(), atom()) ->
    {ok, term()} | error.
read(#shu{key_to_slot = K2S, cfg = Cfg} = State, Key, FieldName) ->
    case K2S of
        #{Key := SlotIdx} ->
            case lookup_field(FieldName, Cfg) of
                {ok, Field} ->
                    read_field(SlotIdx, Field, State);
                error ->
                    error
            end;
        _ ->
            error
    end.

-spec read_all(state(), Key :: binary()) ->
    {ok, #{atom() => term()}} | error.
read_all(#shu{key_to_slot = K2S,
              cfg = #cfg{fields = Fields,
                        record_size = RecordSize} = Cfg,
              fd = Fd} = State,
         Key) ->
    case K2S of
        #{Key := SlotIdx} ->
            %% Read entire record at once for efficiency
            Pos = record_pos(Cfg, SlotIdx),
            case file:pread(Fd, Pos, RecordSize) of
                {ok, RecordBin} ->
                    try
                        Result = lists:foldl(
                            fun(#field{name = Name, frequency = high} = F,
                                Acc) ->
                                    %% High-frequency fields might be in
                                    %% WAL/ETS, check there first
                                    case read_field(SlotIdx, F, State) of
                                        {ok, Value} ->
                                            Acc#{Name => Value};
                                        error ->
                                            %% Field not found in WAL/file,
                                            %% treat as undefined
                                            Acc#{Name => undefined}
                                    end;
                               (#field{name = Name,
                                       offset = FieldOffset,
                                       size = FieldSize} = F,
                                Acc) ->
                                    %% Low-frequency fields are in record
                                    <<_:FieldOffset/binary,
                                      FieldBin:FieldSize/binary,
                                      _/binary>> = RecordBin,
                                    {Decoded, _} =
                                        decode_field_value(F, FieldBin, State),
                                    Acc#{Name => Decoded}
                            end, #{}, Fields),
                        {ok, Result}
                    catch
                        _:_ ->
                            %% Decoding error (e.g., missing atom in atom table)
                            %% indicates corrupted or partially written data
                            error
                    end;
                {error, _} ->
                    error
            end;
        _ ->
            error
    end.

read_field(SlotIdx, #field{frequency = high, id = FieldId} = Field,
           #shu{wal_tab = Tab} = State) ->
    case ets:lookup(Tab, {SlotIdx, FieldId}) of
        [{_, ValueBin, _Seq}] ->
            {Decoded, _} = decode_field_value(Field, ValueBin, State),
            {ok, Decoded};
        [] ->
            read_field_from_file(SlotIdx, Field, State)
    end;
read_field(SlotIdx, Field, State) ->
    read_field_from_file(SlotIdx, Field, State).

read_field_from_file(SlotIdx, #field{size = Size} = Field,
                     #shu{cfg = Cfg, fd = Fd} = State) ->
    Pos = field_pos(Cfg, SlotIdx, Field),
    case file:pread(Fd, Pos, Size) of
        {ok, Bin} ->
            {Decoded, _} = decode_field_value(Field, Bin, State),
            {ok, Decoded};
        {error, _} ->
            error
    end.

%%% ============================================================
%%% Deletes
%%% ============================================================

-spec delete(state(), Key :: binary()) ->
    {ok, state()} | {error, term()}.
delete(#shu{cfg = Cfg, fd = Fd, key_to_slot = K2S,
             free_slots = Free, slot_count = SC,
             wal_tab = Tab, wal_count = WC} = State, Key) ->
    case K2S of
        #{Key := SlotIdx} ->
            Pos = key_index_pos(Cfg, SlotIdx),
            ok = file:pwrite(Fd, Pos, <<?SLOT_DELETED:8>>),
            Deleted = lists:foldl(
                        fun(#field{id = FieldId}, Acc) ->
                                case ets:member(Tab, {SlotIdx, FieldId}) of
                                    true ->
                                        ets:delete(Tab, {SlotIdx, FieldId}),
                                        Acc + 1;
                                    false ->
                                        Acc
                                end
                        end, 0, Cfg#cfg.fields),
            ok = file:sync(Fd),
            {ok, State#shu{key_to_slot = maps:remove(Key, K2S),
                           free_slots = [SlotIdx | Free],
                           slot_count = SC - 1,
                           wal_count = WC - Deleted}};
        _ ->
            {error, not_found}
    end.

%%% ============================================================
%%% Sync
%%% ============================================================

-spec sync(state()) -> {ok, state()} | {error, term()}.
sync(#shu{compacting = true} = _State) ->
    %% Cannot sync during compaction because the WAL region is being
    %% reorganized and file layout is inconsistent. Pending writes are
    %% buffered and will be flushed when compaction completes.
    {error, compaction_in_progress};
sync(#shu{fd = Fd} = State) ->
    %% Normal operation: all writes go directly to disk, so pending_wal
    %% is always empty. Just sync the file descriptor.
    case file:sync(Fd) of
        ok -> {ok, State};
        {error, _} = Err -> Err
    end.

%%% ============================================================
%%% Compaction (two-phase)
%%% ============================================================

-spec prepare_compact(state()) -> {compact_work(), state()}.
prepare_compact(#shu{cfg = Cfg, wal_tab = Tab,
                      atom_to_idx = A2I,
                      idx_to_atom = I2A} = State) ->
    Entries = ets:tab2list(Tab),
    Work = #{filename => Cfg#cfg.filename,
             cfg => Cfg,
             entries => Entries,
             atom_to_idx => A2I,
             idx_to_atom => I2A},
    {Work, State#shu{compacting = true, pending_wal = []}}.

-spec do_compact(compact_work()) -> compact_result().
do_compact(#{filename := Filename, cfg := Cfg,
             entries := Entries}) ->
    {ok, Fd} = file:open(Filename, [read, write, raw, binary]),
    try
        %% Build a map for O(1) field lookups instead of O(N*M) linear search
        FieldMap = maps:from_list(
                     [{FieldId, Field} ||
                      #field{id = FieldId} = Field <- Cfg#cfg.fields]),
        PWrites = lists:filtermap(
                    fun({{SlotIdx, FieldId}, ValueBin, _Seq}) ->
                            case maps:find(FieldId, FieldMap) of
                                {ok, #field{size = Size} = Field} ->
                                    Pos = field_pos(Cfg, SlotIdx, Field),
                                    Bin = fit_to_size(ValueBin, Size),
                                    {true, {Pos, Bin}};
                                error ->
                                    false
                            end
                    end, Entries),
        %% Sort pwrite operations by position for better I/O locality
        SortedWrites = lists:keysort(1, PWrites),
        case SortedWrites of
            [] -> ok;
            _ -> ok = file:pwrite(Fd, SortedWrites)
        end,
        ok = file:sync(Fd),
        ok
    catch
        _:Reason ->
            {error, Reason}
    after
        file:close(Fd)
    end.

-spec finish_compact(compact_result(), state()) ->
    {ok, state()} | {error, term()}.
finish_compact({error, _} = Err, _State) ->
    Err;
finish_compact(ok, #shu{cfg = Cfg, fd = Fd, wal_tab = Tab,
                         pending_wal = Pending} = State) ->
    #cfg{wal_offset = WalOff} = Cfg,
    {ok, _} = file:position(Fd, WalOff),
    ok = file:truncate(Fd),
    ok = file:sync(Fd),
    true = ets:delete_all_objects(Tab),
    ReversedPending = lists:reverse(Pending),
    State1 = State#shu{wal_pos = 0, wal_seq = 0,
                        wal_count = 0, compacting = false,
                        pending_wal = []},
    State2 = replay_pending_wal(ReversedPending, Cfg, Fd, Tab, State1),
    ok = file:sync(Fd),
    {ok, State2}.

replay_pending_wal(Pending, Cfg, Fd, Tab, State) ->
    #cfg{wal_capacity = WalCap, wal_entry_size = EntrySize} = Cfg,
    {State1, PWrites} =
        lists:foldl(
          fun(Entry, {S, Acc}) ->
                  #shu{wal_pos = WalPos, wal_count = WC} = S,
                  ActualPos = WalPos rem WalCap,
                  FilePos = wal_entry_pos(Cfg, ActualPos),
                  <<SlotIdx:32/unsigned-big, FieldId:8, ValueAndMeta/binary>> = Entry,
                  ValueSize = EntrySize - 4 - 1 - 8,
                  <<Value:ValueSize/binary, Seq:64/unsigned-big>> = ValueAndMeta,
                  EtsKey = {SlotIdx, FieldId},
                  IsNew = not ets:member(Tab, EtsKey),
                  true = ets:insert(Tab, {EtsKey, Value, Seq}),
                  S1 = S#shu{wal_pos = WalPos + 1,
                             wal_seq = Seq,
                             wal_count = WC + (case IsNew of
                                                   true -> 1;
                                                   false -> 0
                                               end)},
                  {S1, [{FilePos, Entry} | Acc]}
          end, {State, []}, Pending),
    case PWrites of
        [] -> ok;
        _ -> ok = file:pwrite(Fd, lists:reverse(PWrites))
    end,
    State1.

fit_to_size(Bin, Size) ->
    case byte_size(Bin) of
        S when S >= Size ->
            <<V:Size/binary, _/binary>> = Bin,
            V;
        S ->
            Pad = Size - S,
            <<Bin/binary, 0:(Pad * 8)>>
    end.

%%% ============================================================
%%% Info
%%% ============================================================

-spec info(state()) -> #{atom() => term()}.
info(#shu{cfg = Cfg, slot_count = SC, atom_count = AC,
           wal_count = WC, wal_pos = WP, compacting = Comp}) ->
    #cfg{num_slots = NumSlots, wal_capacity = WalCap} = Cfg,
    #{slot_count => SC,
      num_slots => NumSlots,
      atom_count => AC,
      wal_count => WC,
      wal_pos => WP,
      wal_capacity => WalCap,
      wal_usage => WC / WalCap,
      compacting => Comp}.

%%% ============================================================
%%% Fold
%%% ============================================================

-spec fold(fun((Key :: binary(), Fields :: #{atom() => term()}, Acc) -> Acc),
           Acc, state()) ->
    Acc.
fold(Fun, Acc0, State) ->
    fold(Fun, Acc0, State, 65536).

-spec fold(fun((Key :: binary(), Fields :: #{atom() => term()}, Acc) -> Acc),
           Acc, state(), pos_integer()) ->
    Acc.
fold(Fun, Acc0, State, ReadaheadBytes) ->
    #shu{key_to_slot = KeyToSlot, cfg = Cfg, fd = Fd} = State,
    %% Create sorted list of {SlotIdx, Key} for sequential access
    KeySlotList = lists:sort([{S, K} || {K, S} <- maps:to_list(KeyToSlot)]),
    fold_loop(Fun, Acc0, State, KeySlotList, Cfg, Fd, ReadaheadBytes, 0).

fold_loop(_Fun, Acc, _State, [], _Cfg, _Fd, _ReadaheadBytes, _BufferStart) ->
    Acc;
fold_loop(Fun, Acc, State, KeySlotList, Cfg, Fd, ReadaheadBytes, _BufferStart) ->
    %% Determine which records to read in this batch
    {BatchSlots, RestKeys} = gather_batch(KeySlotList, Cfg, ReadaheadBytes, []),
    case BatchSlots of
        [] ->
            Acc;
        _ ->
            %% Calculate byte range for this batch
            {FirstSlot, _} = hd(BatchSlots),
            {LastSlot, _} = lists:last(BatchSlots),
            #cfg{record_offset = RecordOffset, record_size = RecordSize} = Cfg,
            StartPos = RecordOffset + FirstSlot * RecordSize,
            EndPos = RecordOffset + (LastSlot + 1) * RecordSize,
            BatchSize = EndPos - StartPos,
            %% Read batch into memory
            case file:pread(Fd, StartPos, BatchSize) of
                {ok, BatchBin} ->
                    %% Process each record in the batch with decoded fields
                    Acc1 = lists:foldl(
                             fun({SlotIdx, Key}, AccIn) ->
                                     case decode_record_from_buffer(SlotIdx,
                                                                    BatchBin,
                                                                    FirstSlot,
                                                                    Cfg,
                                                                    State) of
                                         {ok, Fields} ->
                                             Fun(Key, Fields, AccIn);
                                         error ->
                                             AccIn
                                     end
                             end, Acc, BatchSlots),
                    fold_loop(Fun, Acc1, State, RestKeys, Cfg, Fd,
                             ReadaheadBytes, FirstSlot);
                {error, _} ->
                    %% On read error, skip this batch
                    fold_loop(Fun, Acc, State, RestKeys, Cfg, Fd,
                             ReadaheadBytes, FirstSlot)
            end
    end.

%% Gather keys up to ReadaheadBytes worth of records
gather_batch([], _Cfg, _ReadaheadBytes, Acc) ->
    {lists:reverse(Acc), []};
gather_batch(KeySlotList, #cfg{record_size = RecordSize} = Cfg, ReadaheadBytes, Acc) ->
    TotalBytes = length(Acc) * RecordSize,
    case TotalBytes >= ReadaheadBytes of
        true ->
            {lists:reverse(Acc), KeySlotList};
        false ->
            [H | T] = KeySlotList,
            gather_batch(T, Cfg, ReadaheadBytes, [H | Acc])
    end.

%% Decode record from readahead buffer into field map
decode_record_from_buffer(SlotIdx, BatchBin, FirstSlot, Cfg, State) ->
    Offset = (SlotIdx - FirstSlot) * Cfg#cfg.record_size,
    case catch binary:part(BatchBin, Offset, Cfg#cfg.record_size) of
        RecordBin when is_binary(RecordBin) ->
            try
                Fields = lists:foldl(
                           fun(#field{name = Name, frequency = high} = F, Acc) ->
                                   %% High-frequency fields might be in WAL/ETS,
                                   %% check there first, fall back to buffer
                                   case read_field(SlotIdx, F, State) of
                                       {ok, Value} ->
                                           Acc#{Name => Value};
                                       error ->
                                           Acc#{Name => undefined}
                                   end;
                              (#field{name = Name, offset = FieldOffset,
                                      size = FieldSize} = F, Acc) ->
                                   <<_:FieldOffset/binary,
                                     FieldBin:FieldSize/binary,
                                     _/binary>> = RecordBin,
                                   {Decoded, _} = decode_field_value(F, FieldBin, State),
                                   Acc#{Name => Decoded}
                           end, #{}, Cfg#cfg.fields),
                {ok, Fields}
            catch
                _:_ ->
                    error
            end;
        _ ->
            error
    end.

-spec migrate(state(), schema()) ->
    {ok, state()} | {error, term()}.
migrate(OldState, NewSchema) ->
    %% Validate new schema
    case validate_schema(NewSchema) of
        {ok, NewCfg} ->
            OldCfg = OldState#shu.cfg,
            OldFilename = OldCfg#cfg.filename,
            TempFilename = OldFilename ++ ".tmp",
            do_migrate(OldState, OldFilename, TempFilename,
                      NewCfg, NewSchema);
        {error, _} = Err ->
            Err
    end.

do_migrate(OldState, OldFilename, TempFilename, _NewCfg, NewSchema) ->
    %% Close old state temporarily to avoid file conflicts
    ok = file:sync(OldState#shu.fd),
    try
        %% Create new file with larger schema
        case open(TempFilename, NewSchema) of
            {ok, NewState} ->
                %% Copy all entries from old to new
                case copy_all_entries(OldState, NewState) of
                    {ok, NewState2} ->
                        %% Close both files
                        ok = close(NewState2),
                        %% Atomically swap files
                        case prim_file:rename(TempFilename, OldFilename) of
                            ok ->
                                %% Reopen with new schema
                                {ok, MigratedState} =
                                    open(OldFilename, NewSchema),
                                {ok, MigratedState};
                            {error, Reason} ->
                                {error, {rename_failed, Reason}}
                        end;
                    {error, Reason} ->
                        _ = close(NewState),
                        _ = prim_file:delete(TempFilename),
                        {error, {copy_failed, Reason}}
                end;
            {error, Reason} ->
                {error, {create_failed, Reason}}
        end
    catch
        _:Error ->
            _ = prim_file:delete(TempFilename),
            {error, {migration_failed, Error}}
    end.

copy_all_entries(OldState, NewState) ->
    %% Iterate all keys and copy their data
    Entries = maps:to_list(OldState#shu.key_to_slot),
    copy_entries_loop(Entries, OldState, NewState).

copy_entries_loop([], _OldState, NewState) ->
    {ok, NewState};
copy_entries_loop([{Key, _SlotIdx} | Rest], OldState, NewState) ->
    %% Read all fields for this key
    case read_all(OldState, Key) of
        {ok, Fields} ->
            %% Convert fields map to list for write
            FieldList = maps:to_list(Fields),
            case write(NewState, Key, FieldList) of
                {ok, NewState1} ->
                    copy_entries_loop(Rest, OldState, NewState1);
                {wal_full, NewState1} ->
                    %% Compaction needed during migration
                    {Work, NewState2} = prepare_compact(NewState1),
                    case do_compact(Work) of
                        ok ->
                            case finish_compact(ok, NewState2) of
                                {ok, NewState3} ->
                                    copy_entries_loop(Rest, OldState,
                                                     NewState3);
                                {error, _} = Err ->
                                    Err
                            end;
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        error ->
            %% Key not found, shouldn't happen but continue
            copy_entries_loop(Rest, OldState, NewState)
    end.

%%% ============================================================
%%% Internal: flush pending WAL to disk
%%% ============================================================

-spec flush_pending_wal(#shu{}) -> #shu{}.
flush_pending_wal(#shu{pending_wal = []} = State) ->
    State;
flush_pending_wal(#shu{pending_wal = Pending, cfg = Cfg, fd = Fd,
                        wal_pos = WalPos0} = State) ->
    #cfg{wal_capacity = WalCap} = Cfg,
    ReversedPending = lists:reverse(Pending),
    {NewWalPos, PWrites} =
        lists:foldl(
          fun(Entry, {Pos, Acc}) ->
                  ActualPos = Pos rem WalCap,
                  FilePos = wal_entry_pos(Cfg, ActualPos),
                  {Pos + 1, [{FilePos, Entry} | Acc]}
          end, {WalPos0, []}, ReversedPending),
    case PWrites of
        [] -> ok;
        _ -> ok = file:pwrite(Fd, lists:reverse(PWrites))
    end,
    State#shu{pending_wal = [], wal_pos = NewWalPos}.
