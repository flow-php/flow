# Docker

- [⬅️️ Back](../installation.md)

Since some of the Flow adapters require additional PHP extensions, we have prepared a Docker image with all the necessary dependencies.

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

If you would like to try Flow, fork this repository, navigate to it through command line interface and execute following command:

```shell
$ docker run -v $(pwd):/flow-workspace --rm -it flow-php/flow:latest run /flow-workspace/examples/topics/aggregations/daily_revenue.php
```

Flow CLI will grab the pipeline definition from `examples/topics/aggregations/daily_revenue.php` file and execute it.

