# Flow Command Line Interface


## Installation

``` 
composer require flow-php/cli
```

In some cases, it might make sense to install the CLI globally:

```
composer global require flow-php/cli
```

Now you can run the CLI using the `flow` command.

## Commands

### Config 

All Flow CLI Commands can be configured using `--config` option. The option accepts a path to a configuration file in php that returns an Config or ConfigBuilder instance.

`.flow.php`

```php
<?php

use function Flow\ETL\DSL\config_builder;

return config_builder()
    ->id('execution-id');
```

`flow read --config .flow.php orders.csv`

One of the most common use cases is to mount custom filesystem into Flow fstab to access remote files through CLI.

```shell
$ flow
Flow PHP - Data processing framework

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
 file
  file:read              [read] Read data from a file.
  file:rows:count        [count] Read data schema from a file.
  file:schema            [schema] Read data schema from a file.
 parquet
  parquet:read           [parquet:read:data] Read data from parquet file
  parquet:read:metadata  Read metadata from parquet file
 pipeline
  pipeline:run           [run] Execute ETL pipeline from a php/json file.
```

### `file:schema`

```shell
$ flow file:schema --help
Description:
  Read data schema from a file.

Usage:
  file:schema [options] [--] <file>
  schema

Arguments:
  file                                         Path to a file from which schema should be extracted.

Options:
      --file-format=FILE-FORMAT                Source file format. When not set file format is guessed from source file path extension
      --file-limit=FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --config=CONFIG.                         Path to a local php file that MUST return instance of: Flow\ETL\Config
      --output-pretty                          Pretty print schema
      --output-table                           Pretty schema as ascii table
      --schema-auto-cast[=SCHEMA-AUTO-CAST]    When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
      --json-pointer=JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --json-pointer-entry-name                When set, JSON Pointer will be used as an entry name in the schema
      --csv-header[=CSV-HEADER]                When set, CSV header will be used as a schema
      --csv-empty-to-null[=CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --csv-separator[=CSV-SEPARATOR]          CSV separator character
      --csv-enclosure[=CSV-ENCLOSURE]          CSV enclosure character
      --csv-escape[=CSV-ESCAPE]                CSV escape character
      --xml-node-path=XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --xml-buffer-size=XML-BUFFER-SIZE        XML buffer size in bytes
      --parquet-columns=PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --parquet-offset=PARQUET-OFFSET          Offset to start reading from
  -h, --help                                   Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                  Do not output any message
  -V, --version                                Display this application version
      --ansi|--no-ansi                         Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                         Do not ask any interactive question
  -v|vv|vvv, --verbose                         Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

Example: 

```shell
$ flow schema orders.csv --table --auto-cast
+------------+----------+----------+-------------+----------+
|       name |     type | nullable | scalar_type | metadata |
+------------+----------+----------+-------------+----------+
|   order_id |     uuid |    false |             |       [] |
| created_at | datetime |    false |             |       [] |
| updated_at | datetime |    false |             |       [] |
|   discount |   scalar |     true |      string |       [] |
|    address |     json |    false |             |       [] |
|      notes |     json |    false |             |       [] |
|      items |     json |    false |             |       [] |
+------------+----------+----------+-------------+----------+
7 rows
```

### `file:read` 

```shell
$ flow read --help
Description:
  Read data from a file.

Usage:
  file:read [options] [--] <file>
  read

Arguments:
  file                                         Path to a file from which schema should be extracted.

Options:
      --file-format=FILE-FORMAT                File format. When not set file format is guessed from source file path extension
      --file-batch-size=FILE-BATCH-SIZE        Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once [default: 100]
      --file-limit=FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --config=CONFIG.                         Path to a local php file that MUST return instance of: Flow\ETL\Config      
      --output-truncate=OUTPUT-TRUNCATE        Truncate output to given number of characters, when set to -1 output is not truncated at all [default: 20]
      --schema-auto-cast[=SCHEMA-AUTO-CAST]    When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
      --json-pointer=JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --json-pointer-entry-name                When set, JSON Pointer will be used as an entry name in the schema
      --csv-header[=CSV-HEADER]                When set, CSV header will be used as a schema
      --csv-empty-to-null[=CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --csv-separator[=CSV-SEPARATOR]          CSV separator character
      --csv-enclosure[=CSV-ENCLOSURE]          CSV enclosure character
      --csv-escape[=CSV-ESCAPE]                CSV escape character
      --xml-node-path=XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --xml-buffer-size=XML-BUFFER-SIZE        XML buffer size in bytes
      --parquet-columns=PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --parquet-offset=PARQUET-OFFSET          Offset to start reading from
  -h, --help                                   Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                  Do not output any message
  -V, --version                                Display this application version
      --ansi|--no-ansi                         Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                         Do not ask any interactive question
  -v|vv|vvv, --verbose                         Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

### `file:rows:count`

```php
$ flow file:rows:count --help
Description:
  Read data schema from a file.

Usage:
  file:rows:count [options] [--] <file>
  count

Arguments:
  file                                         Path to a file from which schema should be extracted.

Options:
      --file-format=FILE-FORMAT                Source file format. When not set file format is guessed from source file path extension
      --file-limit=FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --config=CONFIG.                         Path to a local php file that MUST return instance of: Flow\ETL\Config
      --json-pointer=JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --json-pointer-entry-name                When set, JSON Pointer will be used as an entry name in the schema
      --csv-header[=CSV-HEADER]                When set, CSV header will be used as a schema
      --csv-empty-to-null[=CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --csv-separator[=CSV-SEPARATOR]          CSV separator character
      --csv-enclosure[=CSV-ENCLOSURE]          CSV enclosure character
      --csv-escape[=CSV-ESCAPE]                CSV escape character
      --xml-node-path=XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --xml-buffer-size=XML-BUFFER-SIZE        XML buffer size in bytes
      --parquet-columns=PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --parquet-offset=PARQUET-OFFSET          Offset to start reading from
  -h, --help                                   Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                  Do not output any message
  -V, --version                                Display this application version
      --ansi|--no-ansi                         Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                         Do not ask any interactive question
  -v|vv|vvv, --verbose                         Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

### `parquet:read:metadata`

```shell
$ flow parquet:read:metadata --help
Description:
  Read metadata from parquet file

Usage:
  parquet:read:metadata [options] [--] <file>

Arguments:
  file                  path to parquet file

Options:
      --columns         Display column details
      --row-groups      Display row group details
      --column-chunks   Display column chunks details
      --statistics      Display column chunks statistics details
      --page-headers    Display page headers details
  -h, --help            Display help for the given command. When no command is given display help for the list command
  -q, --quiet           Do not output any message
  -V, --version         Display this application version
      --ansi|--no-ansi  Force (or disable --no-ansi) ANSI output
  -n, --no-interaction  Do not ask any interactive question
  -v|vv|vvv, --verbose  Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

### `pipeline:run`

```shell
$ flow pipeline:run --help
Description:
  Execute ETL pipeline from a php/json file.

Usage:
  pipeline:run [options] [--] <pipeline-file>
  run

Arguments:
  pipeline-file          Path to a php/json with DataFrame definition.

Options:
      --analyze=ANALYZE  Collect processing statistics and print them. [default: false]
      --config=CONFIG Path to a local php file that MUST return instance of: Flow\ETL\Config      
  -h, --help             Display help for the given command. When no command is given display help for the list command
  -q, --quiet            Do not output any message
  -V, --version          Display this application version
      --ansi|--no-ansi   Force (or disable --no-ansi) ANSI output
  -n, --no-interaction   Do not ask any interactive question
  -v|vv|vvv, --verbose   Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug

Help:
  pipeline-file argument must point to a valid php file that returns DataFrame instance.
  Make sure to not execute run() or any other trigger function.
  
  Example of pipeline.php:
  <?php
  return df()
      ->read(from_array([
          ['id' => 1, 'name' => 'User 01', 'active' => true],
          ['id' => 2, 'name' => 'User 02', 'active' => false],
          ['id' => 3, 'name' => 'User 03', 'active' => true],
      ]))
      ->collect()
      ->write(to_output());
```