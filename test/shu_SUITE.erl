%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(shu_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("../src/shu.hrl").

all() ->
    [{group, encoding},
     {group, lifecycle},
     {group, writes},
     {group, reads},
     {group, deletes},
     {group, wal},
     {group, compaction},
     {group, batch},
     {group, fold},
     {group, ra_integration},
     {group, error_paths},
     {group, recovery},
     {group, migration},
     {group, property}].

groups() ->
    [{encoding, [parallel],
      [encode_decode_integer,
       encode_decode_atom,
       encode_decode_binary,
       encode_decode_tuple,
       encode_decode_undefined]},
     {lifecycle, [],
      [open_close,
       open_close_reopen,
       schema_mismatch,
       reopen_preserves_atoms,
       unsupported_version]},
     {writes, [],
      [write_single_low_freq,
       write_single_high_freq,
       write_multi_field,
       write_new_key_allocates_slot,
       write_overwrite_same_key,
       write_undefined_nulls_field,
       write_variable_length_keys]},
     {reads, [],
      [read_low_freq_from_file,
       read_high_freq_from_wal,
       read_all_fields,
       read_not_found,
       read_undefined_fields]},
     {deletes, [],
      [delete_key,
       delete_and_reuse_slot,
       delete_reuse_clears_stale_fields,
       delete_not_found]},
     {wal, [],
      [wal_replay_on_reopen,
       wal_full_signal,
       wal_count_dedup]},
     {compaction, [],
      [compact_two_phase,
       compact_from_other_process,
       compact_with_pending_writes,
       compact_then_reopen,
       delete_then_reopen_orphan_wal,
       delete_during_compaction_no_slot_reuse,
       compact_pending_wal_bounded,
       do_compact_bad_file_returns_error,
       abort_compact_resumes]},
     {batch, [],
      [write_batch_multiple_keys,
       write_batch_single_fsync,
       write_batch_wal_full_continues]},
     {fold, [],
      [fold_low_freq_fields,
       fold_high_freq_fields,
       fold_mixed_fields,
       fold_collects_all_records,
       fold_with_readahead]},
     {ra_integration, [],
      [ra_meta_atomic_term_and_vote,
       ra_meta_variable_uid_keys,
       ra_meta_election_cycle,
       ra_meta_reopen_after_election]},
     {error_paths, [],
      [invalid_key_size,
       store_full_error,
       atom_table_full_error,
       configurable_atom_table_slots,
       unknown_field_error]},
     {recovery, [],
      [atom_count_recovery_from_stale_header,
       crash_recovery_atoms,
       crash_recovery_reuse_no_resurrection,
       orphaned_wal_entry_no_gen_reuse,
       empty_atom_survives_reopen,
       sync_basic]},
     {migration, [],
      [migrate_basic,
       migrate_with_data,
       migrate_during_compaction]},
     {property, [],
      [model_random_sequences]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Dir = ?config(priv_dir, Config),
    File = filename:join(Dir, atom_to_list(TC) ++ ".shu"),
    [{shu_file, File} | Config].

end_per_testcase(_TC, _Config) ->
    ok.

%%% ============================================================
%%% Test schema used across tests
%%% ============================================================

ra_meta_schema() ->
    #{fields => [
        #{name => current_term, type => {integer, 64}, frequency => low},
        #{name => voted_for, type => {tuple, [{atom, 255}, {atom, 255}]},
          frequency => low},
        #{name => last_applied, type => {integer, 64}, frequency => high}
      ],
      key => {binary, 16},
      expected_count => 100}.

make_key(N) ->
    Bin = integer_to_binary(N),
    Pad = 16 - byte_size(Bin),
    <<0:(Pad * 8), Bin/binary>>.

%%% ============================================================
%%% Encoding tests
%%% ============================================================

encode_decode_integer(_Config) ->
    Type = {integer, 64},
    Encoded = shu:encode_value(Type, 42),
    {42, <<>>} = shu:decode_value(Type, Encoded),
    ?assertEqual(9, byte_size(Encoded)).

encode_decode_atom(_Config) ->
    Type = {atom, 255},
    Encoded = shu:encode_value(Type, {atom_idx, 7}),
    {{atom_idx, 7}, <<>>} = shu:decode_value(Type, Encoded),
    ?assertEqual(3, byte_size(Encoded)).

encode_decode_binary(_Config) ->
    Type = {binary, 32},
    Val = <<"hello world">>,
    Encoded = shu:encode_value(Type, Val),
    {Val, <<>>} = shu:decode_value(Type, Encoded),
    ?assertEqual(1 + 2 + 32, byte_size(Encoded)).

encode_decode_tuple(_Config) ->
    Type = {tuple, [{atom, 255}, {atom, 255}]},
    Val = {{atom_idx, 1}, {atom_idx, 2}},
    Encoded = shu:encode_value(Type, Val),
    {Val, <<>>} = shu:decode_value(Type, Encoded),
    ?assertEqual(7, byte_size(Encoded)).

encode_decode_undefined(_Config) ->
    ?assertMatch({undefined, <<>>},
                 shu:decode_value({integer, 64},
                                  shu:encode_value({integer, 64}, undefined))),
    ?assertMatch({undefined, <<>>},
                 shu:decode_value({atom, 255},
                                  shu:encode_value({atom, 255}, undefined))),
    ?assertMatch({undefined, <<>>},
                 shu:decode_value({binary, 16},
                                  shu:encode_value({binary, 16}, undefined))),
    ?assertMatch({undefined, <<>>},
                 shu:decode_value({tuple, [{atom, 255}, {atom, 255}]},
                                  shu:encode_value({tuple, [{atom, 255},
                                                            {atom, 255}]},
                                                   undefined))).

%%% ============================================================
%%% Lifecycle tests
%%% ============================================================

open_close(Config) ->
    File = ?config(shu_file, Config),
    {ok, State} = shu:open(File, ra_meta_schema()),
    Info = shu:info(State),
    ?assertEqual(0, maps:get(slot_count, Info)),
    ok = shu:close(State).

open_close_reopen(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 5),
    ok = shu:close(S1),
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, 5}, shu:read(S2, Key, current_term)),
    ?assertEqual(1, maps:get(slot_count, shu:info(S2))),
    ok = shu:close(S2).

schema_mismatch(Config) ->
    File = ?config(shu_file, Config),
    Schema1 = ra_meta_schema(),
    {ok, S0} = shu:open(File, Schema1),
    ok = shu:close(S0),
    Schema2 = #{fields => [
                    #{name => foo, type => {integer, 64}, frequency => low}
                ],
                key => {binary, 16},
                expected_count => 100},
    ?assertMatch({error, schema_mismatch}, shu:open(File, Schema2)).

reopen_preserves_atoms(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, [{current_term, 1},
                                     {voted_for, {ra, 'ra@host'}}]),
    ok = shu:close(S1),
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, {ra, 'ra@host'}}, shu:read(S2, Key, voted_for)),
    ok = shu:close(S2).

%%% ============================================================
%%% Write tests
%%% ============================================================

write_single_low_freq(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, current_term, 42),
    ?assertMatch({ok, 42}, shu:read(S1, Key, current_term)),
    ok = shu:close(S1).

write_single_high_freq(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, last_applied, 100),
    ?assertMatch({ok, 100}, shu:read(S1, Key, last_applied)),
    ok = shu:close(S1).

write_multi_field(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, [{current_term, 3},
                                     {voted_for, {node1, 'node1@host'}},
                                     {last_applied, 99}]),
    ?assertMatch({ok, 3}, shu:read(S1, Key, current_term)),
    ?assertMatch({ok, {node1, 'node1@host'}}, shu:read(S1, Key, voted_for)),
    ?assertMatch({ok, 99}, shu:read(S1, Key, last_applied)),
    ok = shu:close(S1).

write_new_key_allocates_slot(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S1} = shu:write(S0, K1, current_term, 1),
    {ok, S2} = shu:write(S1, K2, current_term, 2),
    ?assertEqual(2, maps:get(slot_count, shu:info(S2))),
    ?assertMatch({ok, 1}, shu:read(S2, K1, current_term)),
    ?assertMatch({ok, 2}, shu:read(S2, K2, current_term)),
    ok = shu:close(S2).

write_overwrite_same_key(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    {ok, S2} = shu:write(S1, Key, current_term, 2),
    ?assertMatch({ok, 2}, shu:read(S2, Key, current_term)),
    ?assertEqual(1, maps:get(slot_count, shu:info(S2))),
    ok = shu:close(S2).

write_undefined_nulls_field(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, [{current_term, 5},
                                     {voted_for, {ra, 'ra@host'}},
                                     {last_applied, 100}]),
    ?assertMatch({ok, 5}, shu:read(S1, Key, current_term)),
    ?assertMatch({ok, 100}, shu:read(S1, Key, last_applied)),
    ?assertMatch({ok, {ra, 'ra@host'}}, shu:read(S1, Key, voted_for)),
    %% null out low-freq integer field
    {ok, S2} = shu:write(S1, Key, current_term, undefined),
    ?assertMatch({ok, undefined}, shu:read(S2, Key, current_term)),
    %% null out low-freq tuple field -- should return undefined, not {undefined, undefined}
    {ok, S3} = shu:write(S2, Key, voted_for, undefined),
    ?assertMatch({ok, undefined}, shu:read(S3, Key, voted_for)),
    %% null out high-freq field
    {ok, S4} = shu:write(S3, Key, last_applied, undefined),
    ?assertMatch({ok, undefined}, shu:read(S4, Key, last_applied)),
    %% record still exists
    ?assertEqual(1, maps:get(slot_count, shu:info(S4))),
    ok = shu:close(S4).

write_variable_length_keys(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [#{name => value, type => {integer, 64},
                             frequency => low}],
               key => {binary, 24},
               expected_count => 100},
    {ok, S0} = shu:open(File, Schema),
    Short = <<"ab">>,
    Medium = <<"hello_world_key">>,
    Long = <<"a]very]long]key]of]24by">>,
    {ok, S1} = shu:write(S0, Short, value, 1),
    {ok, S2} = shu:write(S1, Medium, value, 2),
    {ok, S3} = shu:write(S2, Long, value, 3),
    ?assertMatch({ok, 1}, shu:read(S3, Short, value)),
    ?assertMatch({ok, 2}, shu:read(S3, Medium, value)),
    ?assertMatch({ok, 3}, shu:read(S3, Long, value)),
    ?assertEqual(3, maps:get(slot_count, shu:info(S3))),
    %% reopen and verify
    ok = shu:close(S3),
    {ok, S4} = shu:open(File, Schema),
    ?assertMatch({ok, 1}, shu:read(S4, Short, value)),
    ?assertMatch({ok, 2}, shu:read(S4, Medium, value)),
    ?assertMatch({ok, 3}, shu:read(S4, Long, value)),
    ok = shu:close(S4).

%%% ============================================================
%%% Read tests
%%% ============================================================

read_low_freq_from_file(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 77),
    ok = shu:close(S1),
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, 77}, shu:read(S2, Key, current_term)),
    ok = shu:close(S2).

read_high_freq_from_wal(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, last_applied, 500),
    ?assertMatch({ok, 500}, shu:read(S1, Key, last_applied)),
    ok = shu:close(S1).

read_all_fields(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, [{current_term, 10},
                                     {voted_for, {ra, 'ra@localhost'}},
                                     {last_applied, 42}]),
    {ok, All} = shu:read_all(S1, Key),
    ?assertEqual(10, maps:get(current_term, All)),
    ?assertEqual({ra, 'ra@localhost'}, maps:get(voted_for, All)),
    ?assertEqual(42, maps:get(last_applied, All)),
    ok = shu:close(S1).

read_not_found(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    ?assertEqual(error, shu:read(S0, make_key(999), current_term)),
    ok = shu:close(S0).

read_undefined_fields(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    %% allocate slot with one field, leave others undefined
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    ?assertMatch({ok, undefined}, shu:read(S1, Key, last_applied)),
    ok = shu:close(S1).

%%% ============================================================
%%% Delete tests
%%% ============================================================

delete_key(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, current_term, 5),
    ?assertMatch({ok, 5}, shu:read(S1, Key, current_term)),
    {ok, S2} = shu:delete(S1, Key),
    ?assertEqual(error, shu:read(S2, Key, current_term)),
    ?assertEqual(0, maps:get(slot_count, shu:info(S2))),
    ok = shu:close(S2).

delete_and_reuse_slot(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S1} = shu:write(S0, K1, current_term, 1),
    {ok, S2} = shu:delete(S1, K1),
    {ok, S3} = shu:write(S2, K2, current_term, 2),
    ?assertEqual(1, maps:get(slot_count, shu:info(S3))),
    ?assertMatch({ok, 2}, shu:read(S3, K2, current_term)),
    ok = shu:close(S3).

delete_not_found(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    ?assertMatch({error, not_found}, shu:delete(S0, make_key(999))),
    ok = shu:close(S0).

%%% ============================================================
%%% WAL tests
%%% ============================================================

wal_replay_on_reopen(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    {ok, S2} = shu:write(S1, Key, last_applied, 999),
    ok = shu:close(S2),
    {ok, S3} = shu:open(File, Schema),
    ?assertMatch({ok, 999}, shu:read(S3, Key, last_applied)),
    ?assertMatch({ok, 1}, shu:read(S3, Key, current_term)),
    ok = shu:close(S3).

wal_full_signal(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [
                   #{name => value, type => {integer, 64}, frequency => high}
               ],
               key => {binary, 4},
               expected_count => 2},
    {ok, S0} = shu:open(File, Schema),
    Key = <<1, 2, 3, 4>>,
    {Result, SF} = fill_wal(S0, Key, 0),
    ?assertEqual(wal_full, Result),
    ok = shu:close(SF).

fill_wal(State, Key, N) ->
    case shu:write(State, Key, value, N) of
        {ok, S1} ->
            fill_wal(S1, Key, N + 1);
        {wal_full, S1} ->
            {wal_full, S1}
    end.

wal_count_dedup(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, last_applied, 1),
    {ok, S2} = shu:write(S1, Key, last_applied, 2),
    {ok, S3} = shu:write(S2, Key, last_applied, 3),
    %% wal_count should be 1 (one unique {slot, field} pair), not 3
    ?assertEqual(1, maps:get(wal_count, shu:info(S3))),
    ok = shu:close(S3).

%%% ============================================================
%%% Compaction tests
%%% ============================================================

compact_two_phase(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    {ok, S2} = shu:write(S1, Key, last_applied, 10),
    {ok, S3} = shu:write(S2, Key, last_applied, 20),
    {ok, S4} = shu:write(S3, Key, last_applied, 30),
    {Work, S5} = shu:prepare_compact(S4),
    ok = shu:do_compact(Work),
    {ok, S6} = shu:finish_compact(ok, S5),
    ?assertEqual(0, maps:get(wal_count, shu:info(S6))),
    ?assertMatch({ok, 30}, shu:read(S6, Key, last_applied)),
    ok = shu:close(S6).

compact_from_other_process(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    {ok, S2} = shu:write(S1, Key, last_applied, 50),
    {Work, S3} = shu:prepare_compact(S2),
    Self = self(),
    spawn_link(fun() ->
                       Result = shu:do_compact(Work),
                       Self ! {compact_done, Result}
               end),
    receive
        {compact_done, ok} -> ok
    after 5000 ->
              ct:fail(compact_timeout)
    end,
    {ok, S4} = shu:finish_compact(ok, S3),
    ?assertMatch({ok, 50}, shu:read(S4, Key, last_applied)),
    ok = shu:close(S4).

compact_with_pending_writes(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 1),
    {ok, S2} = shu:write(S1, Key, last_applied, 10),
    {Work, S3} = shu:prepare_compact(S2),
    %% write while compacting -- goes to pending_wal
    {ok, S4} = shu:write(S3, Key, last_applied, 99),
    ok = shu:do_compact(Work),
    {ok, S5} = shu:finish_compact(ok, S4),
    %% pending write should survive compaction
    ?assertMatch({ok, 99}, shu:read(S5, Key, last_applied)),
    ok = shu:close(S5).

%%% ============================================================
%%% Batch tests
%%% ============================================================

write_batch_multiple_keys(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    K1 = make_key(1),
    K2 = make_key(2),
    K3 = make_key(3),
    {ok, S1} = shu:write_batch(S0, [
        {K1, [{current_term, 1}, {last_applied, 10}]},
        {K2, [{current_term, 2}, {last_applied, 20}]},
        {K3, [{current_term, 3}, {last_applied, 30}]}
    ]),
    ?assertEqual(3, maps:get(slot_count, shu:info(S1))),
    ?assertMatch({ok, 1}, shu:read(S1, K1, current_term)),
    ?assertMatch({ok, 20}, shu:read(S1, K2, last_applied)),
    ?assertMatch({ok, 3}, shu:read(S1, K3, current_term)),
    ok = shu:close(S1).

write_batch_single_fsync(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    %% batch of 10 keys with low-freq writes should still work
    Ops = [{make_key(N), [{current_term, N}]} || N <- lists:seq(1, 10)],
    {ok, S1} = shu:write_batch(S0, Ops),
    ?assertEqual(10, maps:get(slot_count, shu:info(S1))),
    %% verify all written
    lists:foreach(
      fun(N) ->
              ?assertMatch({ok, N}, shu:read(S1, make_key(N), current_term))
      end, lists:seq(1, 10)),
    ok = shu:close(S1).

write_batch_wal_full_continues(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [
                   #{name => value, type => {integer, 64}, frequency => high}
               ],
               key => {binary, 4},
               expected_count => 10},
    {ok, S0} = shu:open(File, Schema),
    %% create many ops to overflow WAL
    Ops = [{<<N:32>>, [{value, N}]} || N <- lists:seq(1, 10)],
    %% write_batch should process all keys even if WAL fills up
    Result = shu:write_batch(S0, Ops),
    case Result of
        {ok, S1} ->
            ok = shu:close(S1);
        {wal_full, S1} ->
            %% all keys should still have been allocated
            ?assert(maps:get(slot_count, shu:info(S1)) > 0),
            ok = shu:close(S1)
    end.

%%% ============================================================
%%% Fold tests
%%% ============================================================

fold_low_freq_fields(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S1} = shu:write(S0, K1, [{current_term, 5}, {voted_for, {ra, 'ra@node1'}}]),
    {ok, S2} = shu:write(S1, K2, [{current_term, 10}, {voted_for, {node2, 'n2@host'}}]),
    {ok, S3} = shu:sync(S2),
    %% Verify keys are stored
    #{slot_count := SlotCount} = shu:info(S3),
    ?assertEqual(2, SlotCount),
    %% Fold should collect all records with their fields
    Records = shu:fold(fun(Key, Fields, Acc) ->
        [#{key => Key, fields => Fields} | Acc]
    end, [], S3),
    ?assertEqual(2, length(Records)),
    ok = shu:close(S3).

fold_high_freq_fields(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    %% Write high-freq field that stays in WAL
    {ok, S1} = shu:write(S0, Key, last_applied, 42),
    {ok, S2} = shu:sync(S1),
    %% Fold should include the high-freq field from WAL
    Records = shu:fold(fun(K, Fields, Acc) ->
        case maps:get(last_applied, Fields, error) of
            42 ->
                [K | Acc];
            _ ->
                Acc
        end
    end, [], S2),
    ?assertEqual(1, length(Records)),
    ok = shu:close(S2).

fold_mixed_fields(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    %% Write both low-freq and high-freq fields
    {ok, S1} = shu:write(S0, Key, [{current_term, 3},
                                     {voted_for, {ra, 'ra@node1'}},
                                     {last_applied, 99}]),
    {ok, S2} = shu:sync(S1),
    %% Fold should return record with all fields
    Result = shu:fold(fun(_K, Fields, Acc) ->
        case {maps:get(current_term, Fields),
              maps:get(voted_for, Fields),
              maps:get(last_applied, Fields)} of
            {3, {ra, 'ra@node1'}, 99} ->
                [complete];
            _ ->
                Acc
        end
    end, [], S2),
    ?assertEqual([complete], Result),
    ok = shu:close(S2).

fold_collects_all_records(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    %% Write 10 records
    S1 = lists:foldl(
        fun(N, S) ->
            Key = make_key(N),
            {ok, S_} = shu:write(S, Key, current_term, N * 10),
            S_
        end, S0, lists:seq(1, 10)),
    {ok, S2} = shu:sync(S1),
    %% Fold should collect all
    Count = shu:fold(fun(_K, _Fields, Acc) ->
        Acc + 1
    end, 0, S2),
    ?assertEqual(10, Count),
    ok = shu:close(S2).

fold_with_readahead(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    %% Write 5 records
    S1 = lists:foldl(
        fun(N, S) ->
            Key = make_key(N),
            {ok, S_} = shu:write(S, Key, [{current_term, N},
                                          {last_applied, N * 100}]),
            S_
        end, S0, lists:seq(1, 5)),
    {ok, S2} = shu:sync(S1),
    %% Fold with explicit readahead should work
    Records = shu:fold(fun(_K, _Fields, Acc) ->
        Acc + 1
    end, 0, S2, 4096),
    ?assertEqual(5, Records),
    ok = shu:close(S2).

%%% ============================================================
%%% Ra integration tests
%%%
%%% These tests model the recommended ra_log_meta schema where
%%% current_term and voted_for are combined into a single
%%% term_and_vote tuple field for atomic writes. Keys are
%%% variable-length ra_uid() binaries.
%%% ============================================================

ra_meta_integration_schema() ->
    #{fields => [#{name => term_and_vote,
                   type => {tuple, [{integer, 64},
                                    {tuple, [{atom, 255},
                                             {atom, 255}]}]},
                   frequency => low},
                 #{name => last_applied,
                   type => {integer, 64},
                   frequency => high}],
      key => {binary, 24},
      expected_count => 1000}.

ra_meta_atomic_term_and_vote(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_integration_schema(),
    Uid = <<"myra_abc123def456">>,
    {ok, S0} = shu:open(File, Schema),
    %% new term, no vote yet
    {ok, S1} = shu:write(S0, Uid, term_and_vote, {1, undefined}),
    ?assertMatch({ok, {1, undefined}}, shu:read(S1, Uid, term_and_vote)),
    %% vote cast -- atomic update of term + vote
    {ok, S2} = shu:write(S1, Uid, term_and_vote,
                          {1, {ra, 'ra@node1'}}),
    ?assertMatch({ok, {1, {ra, 'ra@node1'}}},
                 shu:read(S2, Uid, term_and_vote)),
    %% new term clears vote atomically
    {ok, S3} = shu:write(S2, Uid, term_and_vote, {2, undefined}),
    ?assertMatch({ok, {2, undefined}}, shu:read(S3, Uid, term_and_vote)),
    ok = shu:close(S3).

ra_meta_variable_uid_keys(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_integration_schema(),
    %% ra_uid() is typically prefix (up to 6 bytes) + 12 random chars
    Short = <<"ra_s1">>,
    Medium = <<"myapp_abc123def456">>,
    Long = <<"longprefix_abcdef123456">>,
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Short, term_and_vote, {1, undefined}),
    {ok, S2} = shu:write(S1, Medium, term_and_vote, {2, undefined}),
    {ok, S3} = shu:write(S2, Long, term_and_vote, {3, undefined}),
    ?assertMatch({ok, {1, undefined}}, shu:read(S3, Short, term_and_vote)),
    ?assertMatch({ok, {2, undefined}}, shu:read(S3, Medium, term_and_vote)),
    ?assertMatch({ok, {3, undefined}}, shu:read(S3, Long, term_and_vote)),
    ?assertEqual(3, maps:get(slot_count, shu:info(S3))),
    ok = shu:close(S3).

ra_meta_election_cycle(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_integration_schema(),
    Uid = <<"ra_test_server1">>,
    {ok, S0} = shu:open(File, Schema),
    %% initial state: term 0, no vote, last_applied 0
    {ok, S1} = shu:write(S0, Uid,
                          [{term_and_vote, {0, undefined}},
                           {last_applied, 0}]),
    %% many last_applied updates (high frequency, no fsync)
    S2 = lists:foldl(
           fun(N, S) ->
                   {ok, S_} = shu:write(S, Uid, last_applied, N),
                   S_
           end, S1, lists:seq(1, 100)),
    ?assertMatch({ok, 100}, shu:read(S2, Uid, last_applied)),
    %% election: term bumps, vote cast (low frequency, fsynced)
    {ok, S3} = shu:write(S2, Uid, term_and_vote,
                          {1, {ra, 'ra@node1'}}),
    ?assertMatch({ok, {1, {ra, 'ra@node1'}}},
                 shu:read(S3, Uid, term_and_vote)),
    %% last_applied unaffected
    ?assertMatch({ok, 100}, shu:read(S3, Uid, last_applied)),
    ok = shu:close(S3).

ra_meta_reopen_after_election(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_integration_schema(),
    Uid = <<"ra_persist_test">>,
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Uid,
                          [{term_and_vote, {5, {ra, 'ra@node2'}}},
                           {last_applied, 42}]),
    ok = shu:close(S1),
    %% reopen -- term_and_vote is low-freq so it was fsynced
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, {5, {ra, 'ra@node2'}}},
                 shu:read(S2, Uid, term_and_vote)),
    %% last_applied was in WAL, should be recovered
    ?assertMatch({ok, 42}, shu:read(S2, Uid, last_applied)),
    ok = shu:close(S2).

%%% ============================================================
%%% Error path tests
%%% ============================================================

unsupported_version(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    {ok, S0} = shu:open(File, Schema),
    ok = shu:close(S0),
    %% Manually write a bad version to the header
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    Header = <<?MAGIC,
               999:16/unsigned-big,
               0:32, 0:16, 0:32, 0:32, 0:32, 0:16, 0:16, 0:16, 0:16>>,
    ok = file:pwrite(Fd, 0, Header),
    ok = file:close(Fd),
    %% Reopen should fail
    ?assertMatch({error, {unsupported_version, 999}}, shu:open(File, Schema)).

invalid_key_size(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    %% Try to write a key that's too long (max is 16 for ra_meta_schema)
    TooLongKey = <<0:200>>,
    ?assertMatch({error, {invalid_key_size, _, _}},
                 shu:write(S0, TooLongKey, current_term, 1)),
    ok = shu:close(S0).

store_full_error(Config) ->
    File = ?config(shu_file, Config),
    %% Create a schema with only 2 slots
    Schema = #{fields => [
                   #{name => value, type => {integer, 64}, frequency => low}
               ],
               key => {binary, 4},
               expected_count => 2},
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, <<1, 2, 3, 4>>, value, 1),
    {ok, S2} = shu:write(S1, <<2, 3, 4, 5>>, value, 2),
    %% Third write should fail with store_full
    ?assertMatch({error, store_full}, shu:write(S2, <<3, 4, 5, 6>>, value, 3)),
    ok = shu:close(S2).

atom_table_full_error(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [
                   #{name => atom_field, type => {atom, 255},
                     frequency => low}
               ],
               key => {binary, 4},
               expected_count => 1000},
    {ok, S0} = shu:open(File, Schema),
    %% Fill atom table (default 256 slots)
    %% Use different keys so each write attempts a new atom
    Fill = fun(N, {ok, St}) ->
                   Key = <<N:32/unsigned-big>>,
                   Atom = list_to_atom("atom_" ++ integer_to_list(N)),
                   shu:write(St, Key, atom_field, Atom);
              (_, Acc) -> Acc
           end,
    {ok, S1} = lists:foldl(Fill, {ok, S0}, lists:seq(0, 255)),
    %% Next write with new atom should throw atom_table_full error
    NewAtom = list_to_atom("atom_256"),
    Key256 = <<256:32/unsigned-big>>,
    ?assertThrow({error, atom_table_full},
                 shu:write(S1, Key256, atom_field, NewAtom)),
    ok = shu:close(S1).

unknown_field_error(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    ?assertThrow({unknown_field, nonexistent_field},
                 shu:write(S0, Key, nonexistent_field, 42)),
    ok = shu:close(S0).

%%% ============================================================
%%% Recovery tests
%%% ============================================================

atom_count_recovery_from_stale_header(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    {ok, S0} = shu:open(File, Schema),
    Key = make_key(1),
    %% Write data with atoms to populate atom table
    {ok, S1} = shu:write(S0, Key, voted_for, {ra, 'ra@node1'}),
    Info1 = shu:info(S1),
    ?assert(maps:get(atom_count, Info1) >= 2),
    ok = shu:close(S1),
    
    %% Simulate stale header by setting atom_count to 0
    %% (Note: This will cause atoms to not be loaded, which is expected behavior
    %%  when header count is 0 - only non-zero counts trigger scanning)
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    {ok, Header} = file:pread(Fd, 0, 32),
    <<Before:26/binary, _:16, After:4/binary>> = Header,
    StaledHeader = <<Before/binary, 0:16/unsigned-big, After/binary>>,
    ok = file:pwrite(Fd, 0, StaledHeader),
    ok = file:close(Fd),
    
    %% Reopen - atom table will be empty (by design, when header says 0)
    {ok, S2} = shu:open(File, Schema),
    %% Info should show 0 atoms
    Info2 = shu:info(S2),
    ?assertEqual(0, maps:get(atom_count, Info2)),
    ok = shu:close(S2).

sync_basic(Config) ->
    File = ?config(shu_file, Config),
    {ok, S0} = shu:open(File, ra_meta_schema()),
    Key = make_key(1),
    {ok, S1} = shu:write(S0, Key, current_term, 42),
    {ok, S2} = shu:sync(S1),
    ?assertMatch({ok, 42}, shu:read(S2, Key, current_term)),
    ok = shu:close(S2).

%%% ============================================================
%%% Compaction recovery tests
%%% ============================================================

compact_then_reopen(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, current_term, 5),
    {ok, S2} = shu:write(S1, Key, last_applied, 100),
    {Work, S3} = shu:prepare_compact(S2),
    ok = shu:do_compact(Work),
    {ok, S4} = shu:finish_compact(ok, S3),
    ?assertEqual(0, maps:get(wal_count, shu:info(S4))),
    ok = shu:close(S4),
    
    %% Reopen and verify truncation worked
    {ok, S5} = shu:open(File, Schema),
    ?assertMatch({ok, 5}, shu:read(S5, Key, current_term)),
    ?assertMatch({ok, 100}, shu:read(S5, Key, last_applied)),
    ?assertEqual(0, maps:get(wal_count, shu:info(S5))),
    ok = shu:close(S5).

delete_then_reopen_orphan_wal(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S0} = shu:open(File, Schema),
    %% Write two keys with high-freq field (goes to WAL)
    {ok, S1} = shu:write(S0, K1, last_applied, 10),
    {ok, S2} = shu:write(S1, K2, last_applied, 20),
    ?assertEqual(2, maps:get(wal_count, shu:info(S2))),
    %% Delete first key (its WAL entries become orphaned)
    {ok, S3} = shu:delete(S2, K1),
    ?assertEqual(1, maps:get(wal_count, shu:info(S3))),
    ok = shu:close(S3),
    
    %% Reopen — orphaned WAL should be skipped, only K2's entry recovered
    {ok, S4} = shu:open(File, Schema),
    ?assertEqual(1, maps:get(wal_count, shu:info(S4))),
    ?assertEqual(error, shu:read(S4, K1, last_applied)),
    ?assertMatch({ok, 20}, shu:read(S4, K2, last_applied)),
    ok = shu:close(S4).

%%% ============================================================
%%% Migration tests
%%% ============================================================

migrate_basic(Config) ->
    File = ?config(shu_file, Config),
    OldSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => low}
                  ],
                  key => {binary, 4},
                  expected_count => 10},
    NewSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => low}
                  ],
                  key => {binary, 4},
                  expected_count => 20},
    {ok, S0} = shu:open(File, OldSchema),
    ok = shu:close(S0),
    
    %% Migrate to new schema
    {ok, S1} = shu:open(File, OldSchema),
    {ok, S2} = shu:migrate(S1, NewSchema),
    %% After migration, OldState (S1) is invalid, don't use it
    Info = shu:info(S2),
    ?assertEqual(20, maps:get(num_slots, Info)),
    ok = shu:close(S2).

migrate_with_data(Config) ->
    File = ?config(shu_file, Config),
    OldSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => low}
                  ],
                  key => {binary, 4},
                  expected_count => 10},
    NewSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => low}
                  ],
                  key => {binary, 4},
                  expected_count => 50},
    {ok, S0} = shu:open(File, OldSchema),
    %% Write some data
    {ok, S1} = shu:write(S0, <<1, 2, 3, 4>>, value, 100),
    {ok, S2} = shu:write(S1, <<5, 6, 7, 8>>, value, 200),
    ok = shu:close(S2),
    
    %% Migrate
    {ok, S3} = shu:open(File, OldSchema),
    {ok, S4} = shu:migrate(S3, NewSchema),
    
    %% Verify data survived migration
    ?assertMatch({ok, 100}, shu:read(S4, <<1, 2, 3, 4>>, value)),
    ?assertMatch({ok, 200}, shu:read(S4, <<5, 6, 7, 8>>, value)),
    Info = shu:info(S4),
    ?assertEqual(2, maps:get(slot_count, Info)),
    ok = shu:close(S4),
    
    %% Reopen and verify data
    {ok, S5} = shu:open(File, NewSchema),
    ?assertMatch({ok, 100}, shu:read(S5, <<1, 2, 3, 4>>, value)),
    ?assertMatch({ok, 200}, shu:read(S5, <<5, 6, 7, 8>>, value)),
    ok = shu:close(S5).

migrate_during_compaction(Config) ->
    File = ?config(shu_file, Config),
    OldSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => high}
                  ],
                  key => {binary, 4},
                  expected_count => 10},
    NewSchema = #{fields => [
                      #{name => value, type => {integer, 64}, frequency => high}
                  ],
                  key => {binary, 4},
                  expected_count => 50},
    {ok, S0} = shu:open(File, OldSchema),
    %% Write some data (high freq -> goes to WAL)
    {ok, S1} = shu:write(S0, <<1, 2, 3, 4>>, value, 10),
    {ok, S2} = shu:write(S1, <<5, 6, 7, 8>>, value, 20),
    ?assert(maps:get(wal_count, shu:info(S2)) > 0),
    %% Compact while data is in WAL
    {Work, S3} = shu:prepare_compact(S2),
    ok = shu:do_compact(Work),
    {ok, S4} = shu:finish_compact(ok, S3),
    %% Now migrate
    {ok, S5} = shu:migrate(S4, NewSchema),
    %% Verify data
    ?assertMatch({ok, 10}, shu:read(S5, <<1, 2, 3, 4>>, value)),
    ?assertMatch({ok, 20}, shu:read(S5, <<5, 6, 7, 8>>, value)),
    ok = shu:close(S5).

%%% ============================================================
%%% Compaction concurrency / robustness tests
%%% ============================================================

%% A slot freed by delete/2 during an in-flight compaction must not be reused
%% by a new key until finish_compact/2, otherwise do_compact/1 (working from
%% the pre-compaction snapshot) would write the freed slot's data into the
%% newly-allocated key's record.
delete_during_compaction_no_slot_reuse(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S0} = shu:open(File, Schema),
    %% K1 gets slot 0 with a high-freq last_applied value in the WAL
    {ok, S1} = shu:write(S0, K1, last_applied, 1000),
    {Work, S2} = shu:prepare_compact(S1),
    %% delete K1 during compaction -> its slot must be deferred, not freed
    {ok, S3} = shu:delete(S2, K1),
    %% a brand new key must NOT reuse K1's slot; it only sets a low-freq field
    %% (no last_applied), so a reused slot would surface K1's stale 1000
    {ok, S4} = shu:write(S3, K2, current_term, 7),
    ok = shu:do_compact(Work),
    {ok, S5} = shu:finish_compact(ok, S4),
    ?assertMatch({ok, 7}, shu:read(S5, K2, current_term)),
    %% the key regression assertion: K2 must not have inherited K1's last_applied
    ?assertMatch({ok, undefined}, shu:read(S5, K2, last_applied)),
    ?assertEqual(error, shu:read(S5, K1, current_term)),
    ok = shu:close(S5).

%% High-frequency writes buffered while compacting must be bounded by wal_size
%% so the finish_compact/2 replay (which starts at WAL offset 0) can never
%% overflow the WAL region.
compact_pending_wal_bounded(Config) ->
    File = ?config(shu_file, Config),
    %% one 64-bit high-freq field -> WAL entry size is 4+1+2+9+4 = 20 bytes;
    %% wal_size 80 holds 4 entries
    Schema = #{fields => [
                   #{name => value, type => {integer, 64}, frequency => high}
               ],
               key => {binary, 4},
               expected_count => 4,
               wal_size => 80},
    Key = <<1, 2, 3, 4>>,
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, value, 1),
    {_Work, S2} = shu:prepare_compact(S1),
    %% buffer high-freq writes while compacting; must hit wal_full once the
    %% pending buffer would exceed wal_size
    {Result, _S3} = buffer_until_full(S2, Key, 0, 100),
    ?assertEqual(wal_full, Result).

buffer_until_full(State, _Key, N, Max) when N >= Max ->
    {no_wal_full, State};
buffer_until_full(State, Key, N, Max) ->
    case shu:write(State, Key, value, N) of
        {ok, S} -> buffer_until_full(S, Key, N + 1, Max);
        {wal_full, S} -> {wal_full, S}
    end.

%% do_compact/1 must return {error, _} (not raise) when it cannot open the file.
do_compact_bad_file_returns_error(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, make_key(1), last_applied, 5),
    {Work0, S2} = shu:prepare_compact(S1),
    BadWork = Work0#{filename => "/nonexistent-dir-shu/does/not/exist.shu"},
    ?assertMatch({error, _}, shu:do_compact(BadWork)),
    %% and the store can be recovered via abort_compact/1
    {ok, S3} = shu:abort_compact(S2),
    ?assertEqual(false, maps:get(compacting, shu:info(S3))),
    ok = shu:close(S3).

%% abort_compact/1 returns the store to normal operation after a failed
%% compaction, preserving the buffered high-freq value (still in the WAL cache)
%% and allowing writes/reads to continue.
abort_compact_resumes(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, last_applied, 10),
    {_Work, S2} = shu:prepare_compact(S1),
    {ok, S3} = shu:write(S2, Key, last_applied, 42),
    %% simulate a failed do_compact by aborting instead of finishing
    {ok, S4} = shu:abort_compact(S3),
    ?assertEqual(false, maps:get(compacting, shu:info(S4))),
    %% buffered value is still readable from the WAL cache
    ?assertMatch({ok, 42}, shu:read(S4, Key, last_applied)),
    %% normal operation resumes
    {ok, S5} = shu:write(S4, Key, current_term, 3),
    ?assertMatch({ok, 3}, shu:read(S5, Key, current_term)),
    ok = shu:close(S5).

%% atom_table_slots must be configurable so a caller can raise the cap above
%% the 256 default; distinct atoms beyond 256 then succeed and survive reopen.
configurable_atom_table_slots(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [
                   #{name => atom_field, type => {atom, 255},
                     frequency => low}
               ],
               key => {binary, 4},
               atom_table_slots => 512,
               expected_count => 1000},
    {ok, S0} = shu:open(File, Schema),
    Fill = fun(N, {ok, St}) ->
                   Key = <<N:32/unsigned-big>>,
                   Atom = list_to_atom("cfg_atom_" ++ integer_to_list(N)),
                   shu:write(St, Key, atom_field, Atom);
              (_, Acc) -> Acc
           end,
    %% 300 distinct atoms would exceed the default 256-slot table
    {ok, S1} = lists:foldl(Fill, {ok, S0}, lists:seq(0, 299)),
    ?assertMatch({ok, cfg_atom_299}, shu:read(S1, <<299:32/unsigned-big>>, atom_field)),
    ok = shu:close(S1),
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, cfg_atom_299}, shu:read(S2, <<299:32/unsigned-big>>, atom_field)),
    ?assertMatch({ok, cfg_atom_0}, shu:read(S2, <<0:32/unsigned-big>>, atom_field)),
    ok = shu:close(S2).

%%% ============================================================
%%% Randomized model / property tests
%%% ============================================================

%% Run many randomized sequences of write/delete/compaction operations against
%% a reference model (a plain map), asserting shu matches the model after every
%% operation and across a close+reopen. Interleaves writes and deletes inside
%% the compacting window (where slot-reuse and pending-wal bugs live) and uses a
%% small WAL to force compaction and backpressure frequently.
model_random_sequences(Config) ->
    File = ?config(shu_file, Config),
    _ = rand:seed(exsss, {19, 87, 2026}),
    Schema = #{fields => [#{name => f_lo, type => {integer, 64},
                            frequency => low},
                          #{name => f_hi, type => {integer, 64},
                            frequency => high}],
               key => {binary, 8},
               expected_count => 20,
               wal_size => 200},
    Keys = [model_key(N) || N <- lists:seq(1, 6)],
    _ = [model_iter(File, Schema, Keys) || _ <- lists:seq(1, 300)],
    ok.

model_key(N) ->
    B = integer_to_binary(N),
    Pad = 8 - byte_size(B),
    <<0:(Pad * 8), B/binary>>.

model_iter(File, Schema, Keys) ->
    _ = file:delete(File),
    {ok, S0} = shu:open(File, Schema),
    NOps = rand:uniform(60),
    {S1, Model, Compacting, Work} =
        run_model_ops(S0, #{}, false, undefined, Keys, NOps),
    %% finish any in-flight compaction so the store can be closed/reopened
    S2 = case Compacting of
             true ->
                 {ok, Sx} = shu:finish_compact(shu:do_compact(Work), S1),
                 Sx;
             false ->
                 S1
         end,
    check_model(S2, Model, Keys, pre_reopen),
    ok = shu:close(S2),
    %% persistence: a fresh open must recover exactly the same state
    {ok, S3} = shu:open(File, Schema),
    check_model(S3, Model, Keys, post_reopen),
    ok = shu:close(S3).

run_model_ops(S, Model, Compacting, Work, _Keys, 0) ->
    {S, Model, Compacting, Work};
run_model_ops(S, Model, Compacting, Work, Keys, N) ->
    {S1, Model1, Compacting1, Work1} =
        model_step(pick_model_op(Compacting), S, Model, Compacting, Work, Keys),
    %% invariant must hold after every single step, including mid-compaction
    check_model(S1, Model1, Keys),
    run_model_ops(S1, Model1, Compacting1, Work1, Keys, N - 1).

pick_model_op(true) ->
    %% while compacting, bias toward writes/deletes and occasionally finish
    case rand:uniform(10) of
        1 -> finish;
        2 -> delete;
        3 -> delete;
        _ -> write
    end;
pick_model_op(false) ->
    case rand:uniform(10) of
        1 -> start;
        2 -> delete;
        3 -> delete;
        _ -> write
    end.

model_step(write, S, Model, Compacting, Work, Keys) ->
    Key = lists:nth(rand:uniform(length(Keys)), Keys),
    FVs = random_fields(),
    model_write(S, Model, Compacting, Work, Keys, Key, FVs);
model_step(delete, S, Model, Compacting, Work, _Keys) ->
    Key0 = maps:keys(Model),
    case Key0 of
        [] ->
            {S, Model, Compacting, Work};
        _ ->
            Key = lists:nth(rand:uniform(length(Key0)), Key0),
            {ok, S1} = shu:delete(S, Key),
            {S1, maps:remove(Key, Model), Compacting, Work}
    end;
model_step(start, S, Model, false, _Work, _Keys) ->
    {W, S1} = shu:prepare_compact(S),
    {S1, Model, true, W};
model_step(finish, S, Model, true, Work, _Keys) ->
    {ok, S1} = shu:finish_compact(shu:do_compact(Work), S),
    {S1, Model, false, undefined};
model_step(_, S, Model, Compacting, Work, _Keys) ->
    {S, Model, Compacting, Work}.

%% write, reclaiming the WAL via a full compaction cycle if it is full
model_write(S, Model, Compacting, Work, Keys, Key, FVs) ->
    case shu:write(S, Key, FVs) of
        {ok, S1} ->
            {S1, update_model(Model, Key, FVs), Compacting, Work};
        {wal_full, S1} ->
            S2 = case Compacting of
                     true ->
                         {ok, Sx} = shu:finish_compact(shu:do_compact(Work), S1),
                         Sx;
                     false ->
                         {W, Sp} = shu:prepare_compact(S1),
                         {ok, Sx} = shu:finish_compact(shu:do_compact(W), Sp),
                         Sx
                 end,
            %% WAL reclaimed; retry into the now-empty WAL (not compacting)
            model_write(S2, Model, false, undefined, Keys, Key, FVs)
    end.

random_fields() ->
    Lo = case rand:uniform(5) of
             1 -> [];
             2 -> [{f_lo, undefined}];
             _ -> [{f_lo, rand:uniform(1000000)}]
         end,
    Hi = case rand:uniform(5) of
             1 -> [];
             2 -> [{f_hi, undefined}];
             _ -> [{f_hi, rand:uniform(1000000)}]
         end,
    case Lo ++ Hi of
        [] -> [{f_lo, rand:uniform(1000000)}];
        FVs -> FVs
    end.

update_model(Model, Key, FVs) ->
    Cur = maps:get(Key, Model, #{}),
    New = lists:foldl(fun({F, V}, A) -> A#{F => V} end, Cur, FVs),
    Model#{Key => New}.

check_model(S, Model, Keys) ->
    check_model(S, Model, Keys, unspecified).

check_model(S, Model, Keys, Phase) ->
    lists:foreach(
      fun(Key) ->
              Exp = maps:get(Key, Model, undefined),
              Got = shu:read_all(S, Key),
              case Exp of
                  undefined ->
                      case Got of
                          error -> ok;
                          _ -> erlang:error({model_mismatch, Phase, Key,
                                             expected_absent, Got})
                      end;
                  Fields ->
                      {ok, GotMap} = Got,
                      ELo = maps:get(f_lo, Fields, undefined),
                      EHi = maps:get(f_hi, Fields, undefined),
                      GLo = maps:get(f_lo, GotMap, undefined),
                      GHi = maps:get(f_hi, GotMap, undefined),
                      case ELo =:= GLo andalso EHi =:= GHi of
                          true -> ok;
                          false ->
                              erlang:error({model_mismatch, Phase, Key,
                                            {expected, ELo, EHi},
                                            {got, GLo, GHi}})
                      end
              end
      end, Keys).

%% Regression: a slot reused after delete must not surface the deleted key's
%% record-area field values for fields the new key does not write.
delete_reuse_clears_stale_fields(Config) ->
    File = ?config(shu_file, Config),
    %% expected_count 1 forces the second key to reuse the first slot
    Schema = #{fields => [#{name => a, type => {integer, 64},
                            frequency => low},
                          #{name => b, type => {integer, 64},
                            frequency => high}],
               key => {binary, 8},
               expected_count => 1},
    K1 = <<1:64>>,
    K2 = <<2:64>>,
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, K1, [{a, 111}, {b, 222}]),
    {ok, S2} = shu:delete(S1, K1),
    %% K2 reuses slot 0 and writes only a; b must not be K1's stale 222
    {ok, S3} = shu:write(S2, K2, [{a, 5}]),
    ?assertMatch({ok, 5}, shu:read(S3, K2, a)),
    ?assertMatch({ok, undefined}, shu:read(S3, K2, b)),
    ok = shu:close(S3),
    %% and the cleared state must survive a reopen
    {ok, S4} = shu:open(File, Schema),
    ?assertMatch({ok, 5}, shu:read(S4, K2, a)),
    ?assertMatch({ok, undefined}, shu:read(S4, K2, b)),
    ok = shu:close(S4).

%% Crash recovery for the atom table: atoms written this session must survive a
%% reopen even without a clean close (which is the only other place the header
%% atom_count is persisted). Simulates a crash by opening a second state on the
%% same file without closing the first.
crash_recovery_atoms(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    Key = make_key(1),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, Key, voted_for, {ra, 'ra@node1'}),
    %% crash: reopen without closing S1 (no clean header write on that path)
    {ok, S2} = shu:open(File, Schema),
    ?assertMatch({ok, {ra, 'ra@node1'}}, shu:read(S2, Key, voted_for)),
    ok = shu:close(S2),
    catch shu:close(S1),
    ok.

%% The confirmed crash-window regression: a high-frequency value written for a
%% key, deleted during a compaction, then not truncated (crash/abort before
%% finish), must NOT resurrect onto a key that later reuses the slot - even
%% across a second crash. The per-slot generation guarantees this: the reused
%% slot has a higher generation, so the deleted owner's WAL entry is skipped on
%% replay. Crashes are simulated by opening a fresh state on the same file
%% without a clean close.
crash_recovery_reuse_no_resurrection(Config) ->
    File = ?config(shu_file, Config),
    Schema = ra_meta_schema(),
    K1 = make_key(1),
    K2 = make_key(2),
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, K1, last_applied, 100),
    {ok, S1b} = shu:sync(S1),
    %% start a compaction and delete K1 within it (deferred, no truncation)
    {_Work, S2} = shu:prepare_compact(S1b),
    {ok, _S3} = shu:delete(S2, K1),
    %% crash before finish: reopen from disk
    {ok, S4} = shu:open(File, Schema),
    ?assertEqual(error, shu:read(S4, K1, last_applied)),
    %% a new key reuses K1's freed slot and does NOT write last_applied
    {ok, S5} = shu:write(S4, K2, current_term, 7),
    %% second crash: reopen
    {ok, S6} = shu:open(File, Schema),
    ?assertMatch({ok, 7}, shu:read(S6, K2, current_term)),
    ?assertMatch({ok, undefined}, shu:read(S6, K2, last_applied)),
    ok = shu:close(S6),
    catch shu:close(S5),
    catch shu:close(S4),
    catch shu:close(S1b),
    ok.

%% An empty atom ('') encodes to a zero-length atom-table slot. Recovery must
%% still load it and every atom after it (the header atom_count is
%% authoritative), rather than mistaking the zero-length slot for the end of
%% the table.
empty_atom_survives_reopen(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [#{name => a, type => {atom, 255}, frequency => low}],
               key => {binary, 4},
               expected_count => 10},
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, <<1, 2, 3, 4>>, a, ''),
    {ok, S2} = shu:write(S1, <<5, 6, 7, 8>>, a, hello),
    ok = shu:close(S2),
    {ok, S3} = shu:open(File, Schema),
    ?assertMatch({ok, ''}, shu:read(S3, <<1, 2, 3, 4>>, a)),
    ?assertMatch({ok, hello}, shu:read(S3, <<5, 6, 7, 8>>, a)),
    ok = shu:close(S3).

%% A high-frequency WAL entry is not fsynced, and neither is the key-index write
%% that records its slot's generation, so a crash can persist the WAL entry
%% (tagged gen G) while losing the key-index write. next_gen must still advance
%% past G on recovery (accounting for surviving WAL entries, not just the key
%% index), otherwise a later allocation could re-issue G and the orphaned entry
%% would match the reused slot. This simulates that crash by zeroing a slot's
%% key-index entry on disk while leaving its WAL entry intact.
orphaned_wal_entry_no_gen_reuse(Config) ->
    File = ?config(shu_file, Config),
    Schema = #{fields => [#{name => f, type => {integer, 64}, frequency => high},
                          #{name => g, type => {integer, 64}, frequency => low}],
               key => {binary, 8},
               expected_count => 2},
    KA = <<1:64>>,
    KB = <<2:64>>,
    KC = <<3:64>>,
    {ok, S0} = shu:open(File, Schema),
    {ok, S1} = shu:write(S0, KA, [{g, 1}]),      %% slot 0, generation 0
    {ok, S1b} = shu:sync(S1),
    {ok, S2} = shu:write(S1b, KB, [{f, 999}]),   %% slot 1, generation 1
    {ok, _S2b} = shu:sync(S2),                    %% WAL entry {1, gen1, f, 999} durable
    ok = shu:close(S2),
    %% Simulate the lost key-index write for slot 1 (its WAL entry survives).
    ok = zero_key_index_slot(File, 1, 8),
    {ok, S3} = shu:open(File, Schema),
    ?assertEqual(error, shu:read(S3, KB, f)),
    %% KC reuses slot 1 (the only free slot) and never writes f.
    {ok, S4} = shu:write(S3, KC, [{g, 7}]),
    {ok, _S4b} = shu:sync(S4),
    ok = shu:close(S4),
    {ok, S5} = shu:open(File, Schema),
    ?assertMatch({ok, 7}, shu:read(S5, KC, g)),
    %% The orphaned gen-1 entry must NOT resurrect onto KC.
    ?assertMatch({ok, undefined}, shu:read(S5, KC, f)),
    ok = shu:close(S5).

%% Zero the key-index entry for SlotIdx on disk, deriving the layout from the
%% on-disk header so it does not hard-code shu's default constants.
zero_key_index_slot(File, SlotIdx, MaxKeySize) ->
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    {ok, <<_Magic:4/binary, _Ver:16, _Crc:32, _MaxKey:16, _Rec:32, _Num:32,
           _Wal:32, AtomSlotSize:16/unsigned-big, _AtomCount:16,
           AtomTableSlots:16/unsigned-big, _Resv:16>>} = file:pread(Fd, 0, 32),
    KeyIndexOffset = 32 + AtomTableSlots * AtomSlotSize,
    KiEntrySize = 10 + MaxKeySize,   %% ?KI_OVERHEAD + max_key_size
    Pos = KeyIndexOffset + SlotIdx * KiEntrySize,
    ok = file:pwrite(Fd, Pos, <<0:(KiEntrySize * 8)>>),
    ok = file:sync(Fd),
    ok = file:close(Fd).
