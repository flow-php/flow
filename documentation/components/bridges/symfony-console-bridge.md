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

$progress = console_heartbeat($output);

try {
    df(config_builder()->heartbeat($progress, everySeconds: 1, onFailure: HeartbeatFailure::log))
        ->read(from_csv($path))
        ->sortBy(ref('id'))
        ->write(to_csv($out))
        ->run();
} finally {
    $progress->finish();
}
```

## What it prints

One line, redrawn in place on every beat:

```
 / sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s
```

- A `ConsoleOutputInterface` (the `$output` of a Symfony command) prints to its error output, stderr.
- An output that is not a terminal gets one line per beat.
- At `-q` it prints nothing.
- An output from `$output->section()` is not a `ConsoleOutputInterface`: it prints to stdout, inside that section.

## finish()

Call `finish()` after each run. Call it in a `finally` block, so that a failed run also clears the line.

- On a terminal it clears the status line. Without it, the next `writeln()` of the command lands on the status line.
- On a section it clears the section.
- On an output that is not a terminal it prints nothing, every beat already ended its line.

A beat after `finish()` starts a new status line. The next `finish()` clears it.
