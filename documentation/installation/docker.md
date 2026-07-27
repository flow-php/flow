# Docker

[DOC_LINK:../installation.md]

Since some of the Flow adapters require additional PHP extensions, we have prepared a Docker image with all the
necessary dependencies.

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

To simplify the usage of Flow CLI, you can create an command alias for it:

```
alias flow='docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest'
```

Now you can use Flow CLI as follows:

```shell
flow --help
```

## Running a pipeline

Write a pipeline file that *returns* a `DataFrame` — `run` executes it for you, so do not call
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

Save it as `pipeline.php` and run it through the image:

```shell
$ docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest run /flow-workspace/pipeline.php
```

## Bundled extensions

Alongside PHP 8.5 and the extensions Flow's adapters need — `bcmath`, `gmp`, `pdo_mysql`,
`pdo_pgsql`, `pdo_sqlite`, `pgsql`, and the `brotli`, `lz4`, `snappy`, `zstd` codecs — the image ships four extensions
that Flow detects and uses automatically:

| Extension  | Package                                                                       | Effect when loaded                                                                                                                                     |
|------------|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `flow_php` | [flow-php/flow-php-ext](/documentation/components/extensions/flow-php-ext.md) | `AdaptiveRowHydrator` and `AdaptiveFloeEncoder` run native, fusing every Floe read/write and every raw-scalar hydration into one native call per batch |
| `arrow`    | [flow-php/arrow-ext](/documentation/components/extensions/arrow-ext.md)       | `AdaptiveParquetEngine` selects `ArrowParquetEngine`, so Parquet reads and writes run native                                                           |
| `pg_query` | [flow-php/pg-query-ext](/documentation/components/extensions/pg-query-ext.md) | `Flow\PostgreSql\Parser` becomes usable at all — SQL parsing, normalization and AST manipulation                                                       |
| `protobuf` | `pecl/protobuf`                                                               | `Flow\PostgreSql\Parser` decodes the parse tree in C instead of pure PHP — measured ~69x faster end to end                                             |

`pdo_pgsql` and `pgsql` link libpq 18 from the PGDG repository, matching the PostgreSQL 18 grammar
`pg_query` is built against. PHP 8.5 additionally compiles in `lexbor`, `uri` and Zend OPcache unconditionally.

> [!NOTE]
> `Parser::parse()` decodes a protobuf AST, and protobuf caps message nesting at 100 levels — roughly 23 levels of
> nested subqueries. Deeper SQL fails to decode regardless of whether `protobuf` is loaded; the extension changes
> speed, not that ceiling.

### Opting out of the native path

Engine selection happens per read and per write, so a single step can be pinned to the PHP implementation:

```php
<?php

use Flow\ETL\Row\PhpRowHydrator;
use Flow\Floe\FloeEngine;
use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config_builder, data_frame};
use function Flow\Floe\DSL\to_floe;

return data_frame(config_builder()->hydrator(new PhpRowHydrator()))
    ->read(from_parquet(__DIR__ . '/input.parquet', engine: new PhpParquetEngine()))
    ->write(to_floe(__DIR__ . '/output.floe', engine: FloeEngine::php));
```

To take the whole container off one native path, mount an empty file over that extension's ini:

```shell
$ docker run --rm -v /dev/null:/usr/local/etc/php/conf.d/docker-php-ext-flow_php.ini \
    -v $(pwd):/flow-workspace ghcr.io/flow-php/flow:latest run /flow-workspace/pipeline.php
```

Once a native engine is selected it does **not** silently degrade. Extension failures surface as
`Flow\Floe\Exception\ExtensionException`, which `FloeReader` and `FloeWriter` wrap as
`Flow\Floe\Exception\FloeException`.
