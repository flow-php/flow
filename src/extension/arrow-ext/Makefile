# Flow PHP Arrow Extension Makefile
EXTENSION_DIR := ./ext
MODULE_DIR := $(EXTENSION_DIR)/modules

# PHP build tools
PHP ?= $(shell which php)

# Detect OS for library extension
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    LIB_EXT := dylib
else
    LIB_EXT := so
endif

CARGO_OUTPUT := target/release/libarrow.$(LIB_EXT)
EXTENSION_SO := $(MODULE_DIR)/arrow.so

.PHONY: all build clean test install rebuild

all: build

# Build extension via cargo
build:
	@echo "Building arrow extension (Rust)..."
	cargo build --release
	@mkdir -p $(MODULE_DIR)
	@cp $(CARGO_OUTPUT) $(EXTENSION_SO)
	@echo "Extension built: $(EXTENSION_SO)"

# Run PHPT tests
test: build
	@echo "Running PHPT tests..."
	@failed=0; total=0; passed=0; \
	for f in tests/phpt/*.phpt; do \
		total=$$((total + 1)); \
		test_name=$$(basename "$$f"); \
		tmp="$$(dirname "$$f")/_run_$${test_name%.phpt}.php"; \
		sed -n '/^--FILE--$$/,/^--EXPECT/p' "$$f" | sed '1d;$$d' > "$$tmp"; \
		expected=$$(sed -n '/^--EXPECT\(F\)\{0,1\}--$$/,$$p' "$$f" | sed '1d' | tr -d '\r'); \
		actual=$$($(PHP) -n -d extension=$$(realpath $(EXTENSION_SO)) "$$tmp" 2>&1) || true; \
		actual=$$(echo "$$actual" | tr -d '\r'); \
		rm -f "$$tmp"; \
		if [ "$$actual" = "$$expected" ]; then \
			echo "PASS: $$test_name"; \
			passed=$$((passed + 1)); \
		else \
			echo "FAIL: $$test_name"; \
			echo "  Expected: $$expected"; \
			echo "  Actual:   $$actual"; \
			failed=1; \
		fi; \
	done; \
	echo "$$passed/$$total tests passed"; \
	if [ $$failed -eq 1 ]; then exit 1; fi

# Install to current PHP extension directory
install: build
	@EXT_DIR=$$($(PHP) -r 'echo ini_get("extension_dir");') && \
		cp $(EXTENSION_SO) "$$EXT_DIR/" && \
		echo "Extension installed to $$EXT_DIR/arrow.so"

# Clean all build artifacts
clean:
	cargo clean
	@rm -rf $(MODULE_DIR)

# Full rebuild
rebuild: clean build
