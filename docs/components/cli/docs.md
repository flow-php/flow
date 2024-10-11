# Flow Command Line Interface


## Installation

``` 
composer require flow-php/cli
```

In some cases it might make sense to install the CLI globally:

```
composer global require flow-php/cli
```

Now you can run the CLI using the `flow` command.

## Usage

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
  run                    Execute ETL pipeline from a php/json file.
 file
  file:schema            Read data schema from a file.
 parquet
  parquet:read           [parquet:read:data] Read data from parquet file
  parquet:read:metadata  Read metadata from parquet file
```

### `file:schema`

```shell
$ flow file:schema --help
Description:
  Read data schema from a file.

Usage:
  file:schema [options] [--] <source>
  schema

Arguments:
  source                            Path to a file from which schema should be extracted.

Options:
      --pretty[=PRETTY]             Pretty print schema [default: false]
      --table[=TABLE]               Pretty schema as ascii table [default: false]
      --auto-cast[=AUTO-CAST]       When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type [default: false]
  -h, --help                        Display help for the given command. When no command is given display help for the list command
  -q, --quiet                       Do not output any message
  -V, --version                     Display this application version
      --ansi|--no-ansi              Force (or disable --no-ansi) ANSI output
  -n, --no-interaction              Do not ask any interactive question
  -if, --input-format=INPUT-FORMAT  Source file format. When not set file format is guessed from source file path extension
  -v|vv|vvv, --verbose              Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug
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