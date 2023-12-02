# Homebrew

- [⬅️️ Back](../installation.md)

To install Flow-PHP using [Homebrew](https://brew.sh), you need to [tap](https://docs.brew.sh/Taps) the formula first:

```bash
brew tap flow-php/flow
```

Now you can install Flow using the following command:

```bash
brew install flow-php
```

The `flow` command is now available to run from anywhere in the system:

```console
$ flow -v
Flow-PHP - Extract Transform Load - Data processing framework 0.5.1

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

To upgrade Flow-PHP binary use the following command:

```shell
brew upgrade flow-php
```
