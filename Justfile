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

# Run tests for the landing site, without the published examples.
test-website *args:
    tools/phpunit/vendor/bin/phpunit -c web/landing/phpunit.xml --testsuite=unit,integration,functional {{args}}

# Run every published example through the real playground. Minutes, on demand.
test-examples *args:
    tools/phpunit/vendor/bin/phpunit -c web/landing/phpunit.xml --testsuite=examples {{args}}

# Run phpbench benchmarks (local only; MUST be inside nix-shell). Args are forwarded to `phpbench run`.
# Defaults to `--report=flow-report`; pass your own `--report=...` to override it.
# Examples:
#   just benchmark
#   just benchmark --group=format-parquet
#   just benchmark --store --tag=before   # store a baseline
#   just benchmark --ref=before           # run current code, compare to baseline
benchmark *args:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "{{args}}" == *"--report"* ]]; then
        tools/phpbench/vendor/bin/phpbench run {{args}}
    else
        tools/phpbench/vendor/bin/phpbench run --report=flow-report {{args}}
    fi

# Run all linters: Mago format-check + Mago lint + monorepo validation + GitHub Actions audit.
lint: lint-mago lint-monorepo lint-actions

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

# Audit GitHub Actions workflows (actionlint static checks + zizmor security audit).
# Workflows that live under src/ are split out to their own repos, so they are not
# auto-discovered with the root .github/workflows and must be listed explicitly:
#   - the extension release workflows (custom, full audit);
#   - every per-package subtree-split readonly.yaml (added by hand per package, so all
#     are audited to catch drift; dangerous-triggers is exempted for them in
#     .github/zizmor.yml — they only run `gh pr close`, never check out PR code).
# .github/actions holds the composite actions every workflow depends on; auditing them is
# what catches unpinned or deprecated `uses:` that never appear in a workflow file.
lint-actions:
    #!/usr/bin/env bash
    set -uo pipefail
    rc=0
    extra_workflows=(src/extension/arrow-ext/.github/workflows/release.yml src/extension/flow-php-ext/.github/workflows/release.yml)
    while IFS= read -r workflow; do
        extra_workflows+=("$workflow")
    done < <(find src -path '*/.github/workflows/readonly.yaml' | sort)
    actionlint "${extra_workflows[@]}" || rc=$?
    actionlint || rc=$?
    zizmor --offline .github/workflows .github/actions "${extra_workflows[@]}" || rc=$?
    exit $rc

# Run static analysis (Mago). The monorepo and web/landing are analyzed in separate runs because
# web/landing is a standalone Composer sub-project with its own vendor (see web/landing/mago.toml).
analyze *args:
    tools/mago/vendor/bin/mago guard
    tools/mago/vendor/bin/mago analyze {{args}}
    tools/mago/vendor/bin/mago --workspace web/landing analyze {{args}}

# Auto-fix code style (Mago format + lint --fix) and GitHub Actions findings (zizmor --fix).
fix:
    tools/mago/vendor/bin/mago format
    tools/mago/vendor/bin/mago lint --fix --potentially-unsafe --format-after-fix
    zizmor --fix .github/workflows

# Build the Flow PHAR archive and copy it into the landing site assets.
phar:
    bin/build-phar.sh
    cp ./build/flow.phar ./web/landing/assets/wasm/tools/flow.phar

# Build the WASM artifact (also rebuilds the PHAR).
wasm: && phar
    cd wasm && ./build.sh

# Idempotent — both tools recompress losslessly, so it is safe to re-run over the whole folder.
# Requires nix-shell (oxipng/jpegoptim).
# Losslessly optimize landing-site images in place: oxipng for PNG, jpegoptim for JPEG.
optimize-images:
    #!/usr/bin/env bash
    set -euo pipefail
    images_dir="web/landing/assets/images"
    oxipng --opt max --strip safe --recursive "$images_dir"
    while IFS= read -r -d '' f; do
        jpegoptim --strip-all --all-progressive "$f"
    done < <(find "$images_dir" -type f \( -iname '*.jpg' -o -iname '*.jpeg' \) -print0)

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
