#!/usr/bin/env bash
set -e

CLI_DIR="src/cli"

composer config repositories.local-etl '{"type": "path", "url": "../core/etl", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-csv '{"type": "path", "url": "../adapter/etl-adapter-csv", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-parquet '{"type": "path", "url": "../adapter/etl-adapter-parquet", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-doctrine '{"type": "path", "url": "../adapter/etl-adapter-doctrine", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-json '{"type": "path", "url": "../adapter/etl-adapter-json", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-text '{"type": "path", "url": "../adapter/etl-adapter-text", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-xml '{"type": "path", "url": "../adapter/etl-adapter-xml", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-excel '{"type": "path", "url": "../adapter/etl-adapter-excel", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-adapter-chartjs '{"type": "path", "url": "../adapter/etl-adapter-chartjs", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-openapi-specification '{"type": "path", "url": "../bridge/openapi/specification", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-parquet-viewer '{"type": "path", "url": "../lib/parquet-viewer", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-postgresql '{"type": "path", "url": "../lib/postgresql", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-telemetry '{"type": "path", "url": "../lib/telemetry", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-types '{"type": "path", "url": "../lib/types", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-array-dot '{"type": "path", "url": "../lib/array-dot", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-filesystem '{"type": "path", "url": "../lib/filesystem", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-parquet '{"type": "path", "url": "../lib/parquet", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-snappy '{"type": "path", "url": "../lib/snappy", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-doctrine-dbal-bulk '{"type": "path", "url": "../lib/doctrine-dbal-bulk", "options": {"symlink": false}}' --working-dir="$CLI_DIR"
composer config repositories.local-dremel '{"type": "path", "url": "../lib/dremel", "options": {"symlink": false}}' --working-dir="$CLI_DIR"

rm -rf "$CLI_DIR/vendor"

composer update --no-dev --working-dir="$CLI_DIR"

tools/box/vendor/bin/box compile --config "$CLI_DIR/box.json"

composer config repositories --unset --working-dir="$CLI_DIR"

rm -rf "$CLI_DIR/vendor"
