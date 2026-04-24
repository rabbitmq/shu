%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(shu_bench).

-export([run/0, run/1]).

-define(SHU_FILE, "bench.shu").
-define(DETS_FILE, "bench.dets").

run() ->
    run(10000).

run(N) ->
    io:format("~n=== Starting Benchmark with N = ~p ===~n", [N]),

    %% Cleanup previous runs
    _ = file:delete(?SHU_FILE),
    _ = file:delete(?DETS_FILE),

    %% 1. Setup
    Schema = #{fields => [
                   #{name => id, type => {integer, 64}, frequency => low},
                   #{name => value, type => {binary, 128}, frequency => low},
                   #{name => counter, type => {integer, 64}, frequency => high}
               ],
               key => {binary, 16},
               expected_count => N},

    {ok, Shu} = shu:open(?SHU_FILE, Schema),
    {ok, Dets} = dets:open_file(dets_bench, [{file, ?DETS_FILE}, {type, set}]),

    Keys = [<<(I):128>> || I <- lists:seq(1, N)],
    ValueBin = <<0:1024>>, %% 128 bytes

    %% 2. Benchmark Inserts
    io:format("~n--- Inserts (~p records) ---~n", [N]),
    {ShuInsertTime, Shu1} = timer:tc(fun() ->
        Batch = lists:map(fun(Key) ->
            <<I:128>> = Key,
            {Key, [{id, I}, {value, ValueBin}, {counter, 0}]}
        end, Keys),
        {ok, S1} = shu:write_batch(Shu, Batch),
        S1
    end),
    {ok, Shu2} = shu:sync(Shu1),

    {DetsInsertTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            <<I:128>> = Key,
            dets:insert(Dets, {Key, I, ValueBin, 0})
        end, Keys),
        dets:sync(Dets)
    end),

    print_result("Shu Inserts", ShuInsertTime, N),
    print_result("DETS Inserts", DetsInsertTime, N),

    %% 3. Benchmark Reads
    io:format("~n--- Reads (~p records) ---~n", [N]),
    {ShuReadTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            {ok, _} = shu:read_all(Shu2, Key)
        end, Keys)
    end),

    {DetsReadTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            [_] = dets:lookup(Dets, Key)
        end, Keys)
    end),

    print_result("Shu Reads", ShuReadTime, N),
    print_result("DETS Reads", DetsReadTime, N),

    %% 4. Benchmark High-Frequency Updates (Counter)
    io:format("~n--- Updates (High-Frequency Field, ~p records) ---~n", [N]),
    {ShuUpdateTime, Shu3} = timer:tc(fun() ->
        lists:foldl(fun(Key, S) ->
            {ok, S1} = shu:write(S, Key, [{counter, 1}]),
            S1
        end, Shu2, Keys)
    end),
    {ok, Shu4} = shu:sync(Shu3),

    {DetsUpdateTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            %% DETS requires reading the whole tuple to update a single field,
            %% or overwriting the whole tuple if we already know the values.
            %% We'll simulate knowing the values to give DETS the best case.
            <<I:128>> = Key,
            dets:insert(Dets, {Key, I, ValueBin, 1})
        end, Keys),
        dets:sync(Dets)
    end),

    print_result("Shu Updates", ShuUpdateTime, N),
    print_result("DETS Updates", DetsUpdateTime, N),

    %% 5. Benchmark Mixed Workload (50% Reads, 50% Updates)
    io:format("~n--- Mixed Workload (50% Reads, 50% Updates, ~p operations) ---~n", [N]),
    {ShuMixedTime, Shu5} = timer:tc(fun() ->
        lists:foldl(fun(Key, S) ->
            <<I:128>> = Key,
            if I rem 2 == 0 ->
                {ok, _} = shu:read_all(S, Key),
                S;
               true ->
                {ok, S1} = shu:write(S, Key, [{counter, 2}]),
                S1
            end
        end, Shu4, Keys)
    end),
    {ok, Shu6} = shu:sync(Shu5),

    {DetsMixedTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            <<I:128>> = Key,
            if I rem 2 == 0 ->
                [_] = dets:lookup(Dets, Key);
               true ->
                dets:insert(Dets, {Key, I, ValueBin, 2})
            end
        end, Keys),
        dets:sync(Dets)
    end),

    print_result("Shu Mixed", ShuMixedTime, N),
    print_result("DETS Mixed", DetsMixedTime, N),

    %% 6. Benchmark Fold (Sequential Traversal)
    io:format("~n--- Fold (Sequential Traversal, ~p records) ---~n", [N]),
    {ShuFoldTime, ShuFoldCount} = timer:tc(fun() ->
        shu:fold(fun(_Key, _Fields, Acc) -> Acc + 1 end, 0, Shu6, 65536)
    end),

    {DetsFoldTime, DetsFoldCount} = timer:tc(fun() ->
        dets:foldl(fun({_Key, _, _, _}, Acc) -> Acc + 1 end, 0, Dets)
    end),

    print_result("Shu Fold", ShuFoldTime, N),
    print_result("DETS Fold", DetsFoldTime, N),
    io:format("  Shu fold visited ~p records~n", [ShuFoldCount]),
    io:format("  DETS fold visited ~p records~n", [DetsFoldCount]),

    %% 7. Benchmark Batch High-Frequency Updates
    io:format("~n--- Batch Updates (High-Frequency Field, ~p records) ---~n", [N]),
    {ShuBatchHotTime, Shu7} = timer:tc(fun() ->
        Batch = lists:map(fun(Key) ->
            {Key, [{counter, 3}]}
        end, Keys),
        {ok, S1} = shu:write_batch(Shu6, Batch),
        S1
    end),
    {ok, Shu8} = shu:sync(Shu7),

    {DetsBatchHotTime, _} = timer:tc(fun() ->
        lists:foreach(fun(Key) ->
            <<I:128>> = Key,
            dets:insert(Dets, {Key, I, ValueBin, 3})
        end, Keys),
        dets:sync(Dets)
    end),

    print_result("Shu Batch Hot", ShuBatchHotTime, N),
    print_result("DETS Batch Hot", DetsBatchHotTime, N),

    %% 8. Benchmark High-Frequency Updates (Single Key)
    io:format("~n--- High-Frequency Updates (Single Key, ~p updates) ---~n", [N]),
    HotKey = hd(Keys),
    {ShuHotUpdateTime, Shu9} = timer:tc(fun() ->
        lists:foldl(fun(I, S) ->
            case shu:write(S, HotKey, [{counter, I}]) of
                {ok, S1} -> S1;
                {wal_full, S1} ->
                    {Work, S2} = shu:prepare_compact(S1),
                    ok = shu:do_compact(Work),
                    {ok, S3} = shu:finish_compact(ok, S2),
                    S3
            end
        end, Shu8, lists:seq(1, N))
    end),
    {ok, Shu10} = shu:sync(Shu9),

    {DetsHotUpdateTime, _} = timer:tc(fun() ->
        <<I:128>> = HotKey,
        lists:foreach(fun(J) ->
            dets:insert(Dets, {HotKey, I, ValueBin, J})
        end, lists:seq(1, N)),
        dets:sync(Dets)
    end),

    print_result("Shu Hot Update", ShuHotUpdateTime, N),
    print_result("DETS Hot Update", DetsHotUpdateTime, N),

    %% Teardown
    ok = shu:close(Shu10),
    ok = dets:close(Dets),

    _ = file:delete(?SHU_FILE),
    _ = file:delete(?DETS_FILE),
    ok.

print_result(Name, Microsecs, N) ->
    Secs = Microsecs / 1000000.0,
    OpsPerSec = case Secs of
                    +0.0 -> 0;
                    _ -> N / Secs
                end,
    io:format("~-15s: ~8.2f ms (~10.2f ops/sec)~n", [Name, Microsecs / 1000.0, OpsPerSec]).
