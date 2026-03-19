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
