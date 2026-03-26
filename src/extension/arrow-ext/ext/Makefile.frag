ARROW_CARGO_DIR = $(srcdir)/..

all: cargo_build_arrow

.PHONY: cargo_build_arrow
cargo_build_arrow:
	cd "$(ARROW_CARGO_DIR)" && PHP_CONFIG="$$(which php-config)" PHP="$$(which php)" cargo build --release
	@mkdir -p modules
	@if [ "$$(uname)" = "Darwin" ]; then \
		cp "$(ARROW_CARGO_DIR)/target/release/libarrow.dylib" modules/arrow.so; \
	else \
		cp "$(ARROW_CARGO_DIR)/target/release/libarrow.so" modules/arrow.so; \
	fi
	@echo "Replaced dummy arrow.so with Rust-built extension"
