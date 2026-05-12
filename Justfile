# Flow PHP task runner.
# Run `just` or `just --list` to see available recipes.

set shell := ["bash", "-cu"]

default:
    @just --list

# Install all dependencies (root + tool dirs via composer post-install-cmd hook).
install:
    composer install

# Update all dependencies (root + tool dirs via composer post-update-cmd hook).
update:
    composer update

# Full build pipeline: lint, static analysis, tests, mutation.
build: lint analyze test test-mutation

# Run PHPUnit. Extra arguments are forwarded (e.g. `just test --filter=foo`).
test *args:
    tools/phpunit/vendor/bin/phpunit {{args}}

# Run mutation testing with Infection.
test-mutation *args:
    tools/infection/vendor/bin/infection --threads=max {{args}}

# Run tests for the landing site.
test-website:
    composer test --working-dir=./web/landing

# Run all linters: Mago format-check + Mago lint + monorepo validation.
lint: lint-mago lint-monorepo

# Run Mago format check and lint.
lint-mago:
    tools/mago/vendor/bin/mago format --check
    tools/mago/vendor/bin/mago lint

# Validate the monorepo configuration.
lint-monorepo:
    tools/monorepo/vendor/bin/monorepo-builder validate

# Lint markdown links across the repository.
lint-links:
    docker run -t --rm -v $PWD:/app norberttech/md-link-linter --exclude=vendor --exclude=.scratchpad --exclude=documentation .

# Run static analysis (PHPStan).
analyze *args:
    tools/phpstan/vendor/bin/phpstan analyze -c phpstan.neon --memory-limit=-1 {{args}}

# Run Mago static analyzer, scoped to packages we've finished migrating (add package paths below).
analyze-mago *args:
    tools/mago/vendor/bin/mago analyze {{args}} \
        src/lib/types \
        src/lib/telemetry

# Auto-fix code style with Mago (format + lint --fix).
fix:
    tools/mago/vendor/bin/mago format
    tools/mago/vendor/bin/mago lint --fix --potentially-unsafe --format-after-fix

# Build the Flow PHAR archive and copy it into the landing site assets.
phar:
    bin/build-phar.sh
    cp ./build/flow.phar ./web/landing/assets/wasm/tools/flow.phar

# Build the WASM artifact (also rebuilds the PHAR).
wasm: && phar
    cd wasm && ./build.sh

# Build the Docker image.
docker:
    docker buildx build -t flow-php/flow:latest . --progress=plain --load

# Dump DSL and API JSON used by the landing site.
docs:
    bin/docs.php dsl:dump web/landing/resources/dsl.json
    bin/docs.php api:dump web/landing/resources/api.json

# Generate phpDocumentor API docs for every package config under phpdoc/.
docs-api:
    for cfg in phpdoc/*.xml; do \
        ./tools/phpdocumentor/vendor/bin/phpdoc --config="$cfg" || exit 1; \
    done

# Regenerate Parquet Thrift PHP classes.
gen-thrift: && fix
    grep -q 'namespace php Flow.Parquet.ThriftModel' src/lib/parquet/src/Flow/Parquet/Resources/Thrift/parquet.thrift || { echo "Flow php namespace not found in thrift definition!"; exit 1; }
    rm src/lib/parquet/src/Flow/Parquet/ThriftModel/*.php
    thrift --gen php --out src/lib/parquet/src src/lib/parquet/src/Flow/Parquet/Resources/Thrift/parquet.thrift

# Regenerate PostgreSQL pg_query Protobuf PHP classes.
gen-protobuf-pg: && fix
    rm -rf src/lib/postgresql/src/Flow/PostgreSql/Protobuf
    protoc --php_out=src/lib/postgresql/src --proto_path=src/lib/postgresql/resources/proto pg_query.proto

# Regenerate OpenTelemetry OTLP Protobuf PHP classes.
gen-protobuf-otlp:
    src/bridge/telemetry/otlp/bin/generate-proto.sh
