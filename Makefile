PROJECT = shu
PROJECT_DESCRIPTION = Fixed schema, single file, high throughput durable data store
PROJECT_VERSION = 0.1.0

define PROJECT_ENV
[
]
endef

LOCAL_DEPS = sasl crypto

DIALYZER_OPTS += --src -r test

include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)

# Benchmark targets - only run when explicitly requested
.PHONY: bench
bench:
	rebar3 compile
	erl -pa _build/default/lib/shu/ebin -noshell -eval 'shu_bench:run()' -run init stop

.PHONY: bench-large
bench-large:
	rebar3 compile
	erl -pa _build/default/lib/shu/ebin -noshell -eval 'shu_bench:run(100000)' -run init stop
