# Docker

[DOC_LINK:../installation.md]

Some Flow adapters need additional PHP extensions. For this reason, we made a Docker image with all the necessary
dependencies.

```shell
$ docker pull ghcr.io/flow-php/flow:latest
$ docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest
Flow-PHP - Extract Transform Load - Data processing framework 0.4.0-325-g6c3e4404

Usage:
  command [options] [arguments]

Options:
  -h, --help            Display help for the given command. When no command is given display help for the list command
  -q, --quiet           Do not output any message
  -V, --version         Display this application version
      --ansi|--no-ansi  Force (or disable --no-ansi) ANSI output
  -n, --no-interaction  Do not ask any interactive question
  -v|vv|vvv, --verbose  Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug

Available commands:
  completion             Dump the shell completion script
  help                   Display help for a command
  list                   List commands
  run                    Run ETL pipeline
 parquet
  parquet:read:data      Read data from parquet file
  parquet:read:metadata  Read metadata from parquet file
```

To make the Flow CLI simpler to use, create a command alias for it:

```
alias flow='docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest'
```

Then use the Flow CLI like this:

```shell
flow --help
```

## Running a pipeline

Write a pipeline file that returns a `DataFrame`. `run` executes it for you, so do not call
`->run()` yourself:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

return data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'User 01', 'active' => true],
        ['id' => 2, 'name' => 'User 02', 'active' => false],
    ]))
    ->write(to_output(truncate: false));
```

Save it as `pipeline.php`, and run it through the image:

```shell
$ docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest run /flow-workspace/pipeline.php
```

## Bundled extensions

The image has PHP 8.5 and the extensions that the Flow adapters need: `bcmath`, `gmp`, `zip`, `pdo_mysql`,
`pdo_pgsql`, `pdo_sqlite`, `pgsql`, and the `brotli`, `lz4`, `snappy`, `zstd` codecs. It also has four extensions
that Flow detects and uses automatically:

| Extension  | Package                                                                       | Effect of the loaded extension                                                                                                                                                                                                    |
|------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `flow_php` | [flow-php/flow-php-ext](/documentation/components/extensions/flow-php-ext.md) | `AdaptiveBackend` picks `RustBackend` ([Column Backend](/documentation/components/core/column-backend.md)). CSV and JSON are read and written in Rust. With `arrow`, Parquet batches go between the extensions without PHP values |
| `arrow`    | [flow-php/arrow-ext](/documentation/components/extensions/arrow-ext.md)       | `AdaptiveParquetEngine` picks the `RustParquetEngine` of arrow-ext, so Parquet files are decoded and encoded in Rust                                                                                                              |
| `pg_query` | [flow-php/pg-query-ext](/documentation/components/extensions/pg-query-ext.md) | `Flow\PostgreSql\Parser` becomes usable: SQL parsing, normalization and AST manipulation                                                                                                                                          |
| `protobuf` | `pecl/protobuf`                                                               | `Flow\PostgreSql\Parser` decodes the parse tree in C instead of pure PHP. Measured ~69x faster end to end                                                                                                                         |

`pdo_pgsql` and `pgsql` link libpq 18 from the PGDG repository. This matches the PostgreSQL 18 grammar of
`pg_query`. PHP 8.5 also always compiles in `lexbor`, `uri` and Zend OPcache.

> [!NOTE]
> `Parser::parse()` decodes a protobuf AST, and protobuf limits message nesting to 100 levels. That is approximately
> 23 levels of nested subqueries. Deeper SQL fails to decode, with or without the `protobuf` extension. The extension
> changes the speed, not that limit.

### Opting out of the native path

You choose the backend for each data frame, and the Parquet engine for each read:

```php
<?php

use Flow\ETL\Values\Column\PhpBackend;
use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config_builder, data_frame};
use function Flow\Floe\DSL\to_floe;

return data_frame(config_builder()->backend(new PhpBackend()))
    ->read(from_parquet(__DIR__ . '/input.parquet', engine: new PhpParquetEngine()))
    ->write(to_floe(__DIR__ . '/output.floe'));
```

`PhpBackend` keeps the batches in PHP. But while `flow_php` is loaded, it still parses CSV and JSON files.

To turn off one native path for the full container, mount an empty file over the ini of that extension:

```shell
$ docker run --rm -v /dev/null:/usr/local/etc/php/conf.d/docker-php-ext-flow_php.ini \
    -v $(pwd):/flow-workspace ghcr.io/flow-php/flow:latest run /flow-workspace/pipeline.php
```

After a native path is selected, it does not silently fall back. `flow_php` failures throw
`Flow\ETL\Foundation\Exception\RuntimeException`. A Floe read wraps them as `Flow\ETL\Values\Column\Frame\FloeException`.
arrow-ext failures throw `Flow\Parquet\Exception\*`.
