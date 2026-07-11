# Builds the real extension with cargo and replaces the dummy C artifact
# produced by PHP_NEW_EXTENSION - config.m4 exists only so phpize tooling
# (PIE, pecl) can drive the build. Cargo.toml lives one level above the
# php-ext build-path.

FLOW_PHP_CARGO_DIR = $(srcdir)/..

all: flow-php-cargo

flow-php-cargo: $(all_targets)
	cd $(FLOW_PHP_CARGO_DIR) && $(CARGO) build --release
	@for lib in libflow_php.so libflow_php.dylib; do \
		if test -f "$(FLOW_PHP_CARGO_DIR)/target/release/$$lib"; then \
			cp "$(FLOW_PHP_CARGO_DIR)/target/release/$$lib" modules/flow_php.so; \
			break; \
		fi; \
	done
