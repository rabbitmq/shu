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

    %% Teardown
    ok = shu:close(Shu6),
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
