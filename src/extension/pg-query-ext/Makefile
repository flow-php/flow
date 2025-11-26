# Flow PHP pg_query Extension Makefile
LIBPG_QUERY_VERSION := 17-6.1.0
LIBPG_QUERY_DIR := ./vendor/libpg_query
LIBPG_QUERY_LIB := $(LIBPG_QUERY_DIR)/libpg_query.a
EXTENSION_DIR := ./ext
MODULE_DIR := $(EXTENSION_DIR)/modules
EXTENSION_SO := $(MODULE_DIR)/pg_query.so

# PHP build tools - use shell lookup to ensure we get current PATH values
PHPIZE ?= $(shell which phpize)
PHP_CONFIG ?= $(shell which php-config)
PHP ?= $(shell which php)

# Detect OS for nproc
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    NPROC := $(shell sysctl -n hw.ncpu)
else
    NPROC := $(shell nproc)
endif

.PHONY: all clean distclean test install rebuild

all: build

# Download and build libpg_query
$(LIBPG_QUERY_DIR):
	@echo "Downloading libpg_query $(LIBPG_QUERY_VERSION)..."
	@mkdir -p vendor
	@git clone --depth=1 --branch=$(LIBPG_QUERY_VERSION) \
		https://github.com/pganalyze/libpg_query.git $(LIBPG_QUERY_DIR)

$(LIBPG_QUERY_LIB): $(LIBPG_QUERY_DIR)
	@echo "Building libpg_query..."
	@cd $(LIBPG_QUERY_DIR) && make -j$(NPROC)

# Configure PHP extension (only if configure script doesn't exist)
$(EXTENSION_DIR)/configure: $(LIBPG_QUERY_LIB) $(EXTENSION_DIR)/config.m4
	@echo "Configuring PHP extension..."
	@chmod -R u+w $(EXTENSION_DIR)/build 2>/dev/null || true
	@rm -rf $(EXTENSION_DIR)/build $(EXTENSION_DIR)/run-tests.php
	@cd $(EXTENSION_DIR) && $(PHPIZE)
	@cd $(EXTENSION_DIR) && ./configure \
		--with-pg-query=$(realpath $(LIBPG_QUERY_DIR)) \
		--enable-pg-query

# Build PHP extension (only if .so doesn't exist or sources changed)
$(EXTENSION_SO): $(EXTENSION_DIR)/configure $(EXTENSION_DIR)/pg_query.c $(EXTENSION_DIR)/php_pg_query.h
	@echo "Building PHP extension..."
	@cd $(EXTENSION_DIR) && make -j$(NPROC)
	@echo "Extension built: $(EXTENSION_SO)"

build: $(EXTENSION_SO)

# Run tests
test: $(EXTENSION_SO)
	@echo "Running extension tests..."
	@cd $(EXTENSION_DIR) && make test TESTS="../tests/phpt/"

# Install to current PHP
install: $(EXTENSION_SO)
	@cd $(EXTENSION_DIR) && make install

# Development: rebuild only extension (not libpg_query)
rebuild:
	@cd $(EXTENSION_DIR) && make clean && make

# Clean build artifacts
clean:
	@cd $(EXTENSION_DIR) && ([ -f Makefile ] && make clean || true)
	@chmod -R u+w $(EXTENSION_DIR)/build 2>/dev/null || true
	@rm -rf $(EXTENSION_DIR)/build $(EXTENSION_DIR)/run-tests.php
	@cd $(EXTENSION_DIR) && $(PHPIZE) --clean 2>/dev/null || true

# Remove everything including libpg_query
distclean: clean
	@rm -rf $(LIBPG_QUERY_DIR)
	@rm -rf $(EXTENSION_DIR)/autom4te.cache
	@rm -f $(EXTENSION_DIR)/configure $(EXTENSION_DIR)/config.h.in
