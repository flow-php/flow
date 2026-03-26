# Build the actual extension via Cargo (Rust) and replace the dummy .so
# This runs after PHP's build system compiles the dummy arrow.c

ARROW_CARGO_DIR = $(srcdir)/..

all: cargo_build_arrow

.PHONY: cargo_build_arrow
cargo_build_arrow: $(phplibdir)/arrow.so
	cd "$(ARROW_CARGO_DIR)" && cargo build --release
	@if [ "$$(uname)" = "Darwin" ]; then \
		cp "$(ARROW_CARGO_DIR)/target/release/libarrow.dylib" "$(phplibdir)/arrow.so"; \
	else \
		cp "$(ARROW_CARGO_DIR)/target/release/libarrow.so" "$(phplibdir)/arrow.so"; \
	fi
	@echo "Replaced dummy arrow.so with Rust-built extension"
