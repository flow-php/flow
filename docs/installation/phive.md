# PHIVE

- [⬅️️ Back](../installation.md)

For easier distribution of Flow codebase, we provide PHAR archive signed with GPG, and distributed using [PHIVE](https://phar.io).

```shell
$ phive install flow-php
Phive 0.15.2 - Copyright (C) 2015-2023 by Arne Blankerts, Sebastian Heuer and Contributors
Downloading https://github.com/flow-php/flow/releases/download/0.5.0/flow.phar
Downloading https://github.com/flow-php/flow/releases/download/0.5.0/flow.phar.asc
Downloading key 5A524FD5A2B2C610
Trying to connect to keys.openpgp.org (37.218.245.50)
Downloading https://keys.openpgp.org/pks/lookup?op=get&options=mr&search=0x5A524FD5A2B2C610
Successfully downloaded key.

        Fingerprint: 074C 2154 B220 F32E E37F 3C83 5A52 4FD5 A2B2 C610

        Norbert Orzechowicz (FlowPHP - GPG Key) <contact@norbert.tech>

        Created: 2023-10-31

Import this key? [y|N] y
Linking /Users/stloyd/.phive/phars/flow-php-0.5.0.phar to /Users/stloyd/Documents/flow/tools/flow
$ tools/flow 
Flow-PHP - Extract Transform Load - Data processing framework 0.5.0

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

Now you have link located in `tools/flow`

```shell
tools/flow --help
```
