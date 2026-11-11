---
package: flow-php/symfony-console-bridge
---

# Symfony Console Bridge

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/symfony-console-bridge.md).

## Register

```php
<?php

use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

$output->writeln('  <bg=blue;fg=white> INFO </> Loading orders into the warehouse.');
$progress = console_heartbeat($output);

try {
    df(config_builder()->heartbeat($progress, everySeconds: 0.25, onFailure: HeartbeatFailure::log))
        ->read(from_csv($path))
        ->sortBy(ref('id'))
        ->write(to_csv($out))
        ->run();
} finally {
    $progress->finish();
}
```

The heartbeat prints no header. The command prints its own line before the run.

The styles need a terminal with UTF-8 and colors. Without colors you get `log`:

```bash
NO_COLOR=1 bin/console app:load-orders   # log style, no colors
bin/console app:load-orders --no-ansi    # log style, no colors
bin/console app:load-orders > load.log   # not a terminal: log style
```

## Styles

`console_heartbeat($output)` draws `tasks`. Pass a `ConsoleStyle` case for another style:

```php
<?php

use Flow\Bridge\Symfony\Console\ConsoleStyle;
use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;

$progress = console_heartbeat($output, ConsoleStyle::line);
```

The examples come from a run of 80,000,000 rows at 96 columns. `√` stands for the green check mark that the terminal
prints.

### tasks

`ConsoleStyle::tasks`, the default. One line for each stage, in the order of the pipeline. A running line spins, a
finished line turns into its `DONE` form in place, and a summary ends the run. Use it for a command whose only output
is the run.

While it runs:

```
  √ read      orders-80000000.floe  80,000,000 rows ........................ 9.2M/s · 8.7s DONE
  ⠇ sort      80,000,000 rows .................................................. 6.7M/s · 12.0s
  ⠇ group by  9,363,000 rows .................................................... 2.9M/s · 3.2s
```

At the end:

```
  √ read      orders-80000000.floe  80,000,000 rows ........................ 9.2M/s · 8.7s DONE
  √ sort      80,000,000 rows ............................................. 2.1M/s · 37.3s DONE
  √ group by  10,000 groups ........................................................ 28.6s DONE
  √ commit    warehouse/orders/  40 files ........................................... 0.0s DONE

   DONE  80,000,000 rows in 37.4s · peak 243 MiB · 40 files written
```

A run with a join, 5,000,000 rows, while it reads:

```
  ⠼ read      orders-5000000.floe  1,075,200 / 5,000,000 rows  21% .............. 1.1M/s · 1.0s
  √ read      customers.floe  10,000 rows ........................................... 0.0s DONE
  ⠼ join      1,075,200 rows .................................................... 1.1M/s · 1.0s
  ⠼ sort      1,075,200 rows .................................................... 1.1M/s · 1.0s
```

- A read with an exact total shows a percentage while it runs.
- A rate shows only for rows, and only for a stage that ran at least 0.5 s.
- After it took in every row, a `sort` shows its full row count. It stays open while it merges and emits.
- `group_by` counts the rows it takes in, then the groups it emits.
- On a narrow terminal a line drops columns from the right: memory, rate, graph. Then the count turns short
  (`15.3M/30.0M`), then the line cuts the location, then drops it.
- At `-v` a running line also shows the memory.

### graph

`ConsoleStyle::graph`. The `tasks` style plus a sparkline of the count per second between beats. A finished line keeps
its sparkline. The sparkline needs 90 columns. Below that, the style draws `tasks`. Use it to see a stage slow down.

```
  √ read      orders-80000000.floe  80,000,000 rows ........ ▇▇▇█▆▇▇▇▇▇▆▇ · 15.0M/s · 5.3s DONE
  ⠹ sort      80,000,000 rows ................................... ▁▁▁▁▁▁▁▁▁▁▁▁ · 7.6M/s · 10.5s
  ⠹ group by  15,198,000 rows .................................... ▇▆▇█▇▇▇▇▇▇▇▇ · 3.0M/s · 5.1s
```

### line

`ConsoleStyle::line`. One status line. A finished stage shrinks to `read √ ›`. When the line does not fit, it drops
the locations, then the finished stages, then shows only the latest running stage and `+1 running`. The summary
replaces the line and stays. Use it on a narrow terminal, or in a command that prints its own output between runs.

```
 ⠸ read √ › sort 80.0M rows 9.2M/s · group by 9.5M rows 3.0M/s 8.8s
```

```
 √ 80,000,000 rows in 33.6s · peak 246 MiB · 40 files
```

### log

`ConsoleStyle::log`. Lines that are only appended, each with the time since the start. A stage that ends gets one
line. Every 10 seconds, each running stage gets one line. One summary ends the run. No cursor moves. Use it for CI
and for files.

```
[00:05] read      orders-80000000.floe  done 80,000,000 rows in 6.0s, 13.4M rows/s
[00:10] sort      80,000,000 rows, 8.0M rows/s, memory 35 MiB
[00:10] group by  11,375,000 rows, 2.9M rows/s, memory 35 MiB
[00:20] sort      80,000,000 rows, 4.0M rows/s, memory 33 MiB
[00:20] group by  40,032,000 rows, 2.9M rows/s, memory 33 MiB
[00:30] sort      80,000,000 rows, 2.6M rows/s, memory 37 MiB
[00:30] group by  69,161,000 rows, 2.9M rows/s, memory 37 MiB
[00:34] sort      done 80,000,000 rows in 34.1s, 2.3M rows/s
[00:34] group by  done 10,000 groups in 28.1s
[00:34] commit    warehouse/orders/  done 40 files in 0.0s
[00:34] finished  80,000,000 rows in 34.1s, peak memory 242 MiB, 40 files written
```

## Locations

A read line shows the file or glob that it reads. A commit line shows the directory that it writes to. The location
keeps the last segments: `orders.csv`, `in/*.csv`, `warehouse/orders/`. When a location is too long, the line cuts it
from the left and starts it with an ellipsis. The line never shows a user, password, port, query or fragment.
Two stages with the same name are two lines.

The commit count is files: the files that became visible at their target.

## Rules

- An output that is not decorated gets `log`, whatever the style: not a terminal, `NO_COLOR`, or `--no-ansi`.
- At `-q` the heartbeat prints nothing.
- Build the styles through `console_heartbeat()`. It sends the output of a Symfony command (a `ConsoleOutputInterface`)
  to its error output, stderr. A heartbeat built with `new` draws on the output it gets.
- A section from `$output->section()` gets `line` for `tasks` and `graph`.
- `graph` draws its sparklines only from 90 columns.

## finish()

Call `finish()` after each run, in a `finally` block.

- After a completed run it prints the summary and leaves the finished lines on the screen. Its `peak` is
  `Pulse::$peakMemoryBytes`: in a parallel run, the highest memory of all processes together.
- It always clears the running lines. After a failed run only the finished lines stay.
- A beat after `finish()` starts a new block.
