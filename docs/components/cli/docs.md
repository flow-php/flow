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
  file:convert           [convert] Read data from a file.
  file:read              [read] Read data from a file.
  file:rows:count        [count] Read data schema from a file.
  file:schema            [schema] Read data schema from a file.
 parquet
  parquet:read           [parquet:read:data] Read data from parquet file
  parquet:read:metadata  Read metadata from parquet file
 pipeline
  pipeline:run           [run] Execute ETL pipeline from a php/json file.
```

### `file:convert` alias `convert`

```shell
Description:
  Read data from a file.

Usage:
  file:convert [options] [--] <input-file> <output-file>
  convert

Arguments:
  input-file                                                         Path to a file that should be converted to another format.
  output-file                                                        Path where converted file should be saved.

Options:
      --input-file-format=INPUT-FILE-FORMAT                          File format. When not set file format is guessed from input file path extension
      --input-file-batch-size=INPUT-FILE-BATCH-SIZE                  Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once [default: 100]
      --input-file-limit=INPUT-FILE-LIMIT                            Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --output-file-format=OUTPUT-FILE-FORMAT                        File format. When not set file format is guessed from output file path extension
      --output-overwrite[=OUTPUT-OVERWRITE]                          When set output file will be overwritten if exists
      --schema-auto-cast[=SCHEMA-AUTO-CAST]                          When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
      --analyze[=ANALYZE]                                            Collect processing statistics and print them. [default: false]
      --config=CONFIG                                                Path to a local php file that MUST return instance of: Flow\ETL\Config
      --input-json-pointer=INPUT-JSON-POINTER                        JSON Pointer to a subtree from which schema should be extracted
      --input-json-pointer-entry-name                                When set, JSON Pointer will be used as an entry name in the schema
      --input-csv-header[=INPUT-CSV-HEADER]                          When set, CSV header will be used as a schema
      --input-csv-empty-to-null[=INPUT-CSV-EMPTY-TO-NULL]            When set, empty CSV values will be treated as NULL values
      --input-csv-separator=INPUT-CSV-SEPARATOR                      CSV separator character
      --input-csv-enclosure=INPUT-CSV-ENCLOSURE                      CSV enclosure character
      --input-csv-escape=INPUT-CSV-ESCAPE                            CSV escape character
      --output-csv-header[=OUTPUT-CSV-HEADER]                        When set, CSV header will be used as a schema
      --output-csv-new-line-separator=OUTPUT-CSV-NEW-LINE-SEPARATOR  When set, empty CSV values will be treated as NULL values
      --output-csv-separator=OUTPUT-CSV-SEPARATOR                    CSV separator character
      --output-csv-enclosure=OUTPUT-CSV-ENCLOSURE                    CSV enclosure character
      --output-csv-escape=OUTPUT-CSV-ESCAPE                          CSV escape character
      --output-csv-date-time-format=OUTPUT-CSV-DATE-TIME-FORMAT      DateTime format for CSV output
      --input-xml-node-path=INPUT-XML-NODE-PATH                      XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --input-xml-buffer-size=INPUT-XML-BUFFER-SIZE                  XML buffer size in bytes
      --input-parquet-columns=INPUT-PARQUET-COLUMNS                  Columns to read from parquet file (multiple values allowed)
      --input-parquet-offset=INPUT-PARQUET-OFFSET                    Offset to start reading from
  -h, --help                                                         Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                                        Do not output any message
  -V, --version                                                      Display this application version
      --ansi|--no-ansi                                               Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                                               Do not ask any interactive question
  -v|vv|vvv, --verbose                                               Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

### `file:schema` alias `schema`

```shell
$ flow file:schema --help
Description:
  Read data schema from a file.

Usage:
  file:schema [options] [--] <input-file>
  schema

Arguments:
  input-file                                               Path to a file from which schema should be extracted.

Options:
      --input-file-format=INPUT-FILE-FORMAT                Source file format. When not set file format is guessed from source file path extension
      --input-file-limit=INPUT-FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --output-pretty                                      Pretty print schema
      --output-table                                       Pretty schema as ascii table
      --schema-auto-cast[=SCHEMA-AUTO-CAST]                When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
      --config=CONFIG                                      Path to a local php file that MUST return instance of: Flow\ETL\Config
      --input-json-pointer=INPUT-JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --input-json-pointer-entry-name                      When set, JSON Pointer will be used as an entry name in the schema
      --input-csv-header[=INPUT-CSV-HEADER]                When set, CSV header will be used as a schema
      --input-csv-empty-to-null[=INPUT-CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --input-csv-separator=INPUT-CSV-SEPARATOR            CSV separator character
      --input-csv-enclosure=INPUT-CSV-ENCLOSURE            CSV enclosure character
      --input-csv-escape=INPUT-CSV-ESCAPE                  CSV escape character
      --input-xml-node-path=INPUT-XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --input-xml-buffer-size=INPUT-XML-BUFFER-SIZE        XML buffer size in bytes
      --input-parquet-columns=INPUT-PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --input-parquet-offset=INPUT-PARQUET-OFFSET          Offset to start reading from
  -h, --help                                               Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                              Do not output any message
  -V, --version                                            Display this application version
      --ansi|--no-ansi                                     Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                                     Do not ask any interactive question
  -v|vv|vvv, --verbose                                     Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
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

### `file:read` alias `read`

```shell
$ flow read --help
Description:
  Read data from a file.

Usage:
  file:read [options] [--] <input-file>
  read

Arguments:
  input-file                                               Path to a file from which schema should be extracted.

Options:
      --input-file-format=INPUT-FILE-FORMAT                File format. When not set file format is guessed from source file path extension
      --input-file-batch-size=INPUT-FILE-BATCH-SIZE        Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once [default: 100]
      --input-file-limit=INPUT-FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --output-truncate=OUTPUT-TRUNCATE                    Truncate output to given number of characters, when set to -1 output is not truncated at all [default: 20]
      --schema-auto-cast[=SCHEMA-AUTO-CAST]                When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
      --config=CONFIG                                      Path to a local php file that MUST return instance of: Flow\ETL\Config
      --input-json-pointer=INPUT-JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --input-json-pointer-entry-name                      When set, JSON Pointer will be used as an entry name in the schema
      --input-csv-header[=INPUT-CSV-HEADER]                When set, CSV header will be used as a schema
      --input-csv-empty-to-null[=INPUT-CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --input-csv-separator=INPUT-CSV-SEPARATOR            CSV separator character
      --input-csv-enclosure=INPUT-CSV-ENCLOSURE            CSV enclosure character
      --input-csv-escape=INPUT-CSV-ESCAPE                  CSV escape character
      --input-xml-node-path=INPUT-XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --input-xml-buffer-size=INPUT-XML-BUFFER-SIZE        XML buffer size in bytes
      --input-parquet-columns=INPUT-PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --input-parquet-offset=INPUT-PARQUET-OFFSET          Offset to start reading from
  -h, --help                                               Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                              Do not output any message
  -V, --version                                            Display this application version
      --ansi|--no-ansi                                     Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                                     Do not ask any interactive question
  -v|vv|vvv, --verbose                                     Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
```

### `file:rows:count` alis `count`

```php
$ flow count --help
Description:
  Read data schema from a file.

Usage:
  file:rows:count [options] [--] <input-file>
  count

Arguments:
  input-file                                               Path to a file from which schema should be extracted.

Options:
      --input-file-format=INPUT-FILE-FORMAT                Source file format. When not set file format is guessed from source file path extension
      --input-file-limit=INPUT-FILE-LIMIT                  Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed
      --config=CONFIG                                      Path to a local php file that MUST return instance of: Flow\ETL\Config
      --input-json-pointer=INPUT-JSON-POINTER              JSON Pointer to a subtree from which schema should be extracted
      --input-json-pointer-entry-name                      When set, JSON Pointer will be used as an entry name in the schema
      --input-csv-header[=INPUT-CSV-HEADER]                When set, CSV header will be used as a schema
      --input-csv-empty-to-null[=INPUT-CSV-EMPTY-TO-NULL]  When set, empty CSV values will be treated as NULL values
      --input-csv-separator=INPUT-CSV-SEPARATOR            CSV separator character
      --input-csv-enclosure=INPUT-CSV-ENCLOSURE            CSV enclosure character
      --input-csv-escape=INPUT-CSV-ESCAPE                  CSV escape character
      --input-xml-node-path=INPUT-XML-NODE-PATH            XML node path to a subtree from which schema should be extracted, for example /root/element This is not xpath, just a node names separated by slash
      --input-xml-buffer-size=INPUT-XML-BUFFER-SIZE        XML buffer size in bytes
      --input-parquet-columns=INPUT-PARQUET-COLUMNS        Columns to read from parquet file (multiple values allowed)
      --input-parquet-offset=INPUT-PARQUET-OFFSET          Offset to start reading from
  -h, --help                                               Display help for the given command. When no command is given display help for the list command
  -q, --quiet                                              Do not output any message
  -V, --version                                            Display this application version
      --ansi|--no-ansi                                     Force (or disable --no-ansi) ANSI output
  -n, --no-interaction                                     Do not ask any interactive question
  -v|vv|vvv, --verbose                                     Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
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