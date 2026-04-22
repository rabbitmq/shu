%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-define(MAGIC, "SHU\0").
-define(VERSION, 1).
%% "SHU\0"(4) + version(2) + schema_crc(4) + max_key_size(2) + record_size(4)
%% + num_slots(4) + wal_capacity(4) + atom_slot_size(2) + atom_count(2)
%% + atom_table_slots(2) + reserved(2) = 32
-define(HEADER_SIZE, 32).

-define(SLOT_EMPTY, 0).
-define(SLOT_ACTIVE, 1).
-define(SLOT_DELETED, 2).

-define(DEFAULT_EXPECTED_COUNT, 1024).
-define(DEFAULT_ATOM_TABLE_SLOTS, 256).
-define(WAL_CAPACITY_MULTIPLIER, 4).
-define(WAL_ZERO_CHUNK, 4096).

-type shu_type() :: {integer, 64}
                  | {atom, MaxBytes :: pos_integer()}
                  | {binary, MaxLen :: pos_integer()}
                  | {tuple, [shu_type()]}.

-type frequency() :: low | high.

-type field_spec() :: #{name := atom(),
                        type := shu_type(),
                        frequency := frequency()}.

-type schema() :: #{fields := [field_spec()],
                    key := {binary, pos_integer()},
                    expected_count => pos_integer()}.

-record(field, {name :: atom(),
                id :: non_neg_integer(),
                type :: shu_type(),
                frequency :: frequency(),
                offset :: non_neg_integer(),
                size :: pos_integer()}).

-record(cfg, {filename :: file:filename_all() | undefined,
              schema_crc :: non_neg_integer(),
              max_key_size :: pos_integer(),
              record_size :: pos_integer(),
              num_slots :: pos_integer(),
              fields :: [#field{}],
              field_map :: #{atom() => #field{}},
              high_freq_fields :: [#field{}],
              low_freq_fields :: [#field{}],
              atom_slot_size :: pos_integer(),
              atom_table_slots :: pos_integer(),
              wal_entry_size :: pos_integer(),
              wal_capacity :: pos_integer(),
              atom_table_offset :: pos_integer(),
              key_index_offset :: pos_integer(),
              record_offset :: pos_integer(),
              wal_offset :: pos_integer()}).

-record(shu, {cfg :: #cfg{},
              fd :: file:io_device(),
              slot_count = 0 :: non_neg_integer(),
              key_to_slot = #{} :: #{binary() => non_neg_integer()},
              next_free = 0 :: non_neg_integer(),
              free_slots = [] :: [non_neg_integer()],
              atom_to_idx = #{} :: #{atom() => non_neg_integer()},
              idx_to_atom = #{} :: #{non_neg_integer() => atom()},
              atom_count = 0 :: non_neg_integer(),
              wal_pos = 0 :: non_neg_integer(),
              wal_seq = 0 :: non_neg_integer(),
              wal_count = 0 :: non_neg_integer(),
              wal_tab :: ets:tid(),
              compacting = false :: boolean(),
              pending_wal = [] :: [iodata()]}).

-opaque compact_work() :: map().

-type compact_result() :: ok | {error, term()}.
