---
seo_title: Documentation
seo_description: Strongly typed, memory-efficient data processing for PHP, with installation, quick start, adapters, examples, and references.
---

# Flow PHP

Flow is a strongly typed, memory-efficient data processing framework for PHP.
It gives you one fluent API to read, transform, and write data in CSV,
JSON, XML, Parquet, REST, RDBMS, SEAL, and other formats. You do not write
boilerplate for each format.

This framework is for people who build pipelines, ETL jobs, exports, imports,
or reports in PHP, and for whom one-off scripts are no longer sufficient.

## Hello, Flow

Install the core and the adapters you need:

```bash
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-json
```

Then write your first pipeline:

```php
<?php

use function Flow\ETL\DSL\{data_frame, ref};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\to_json;

data_frame()
    ->read(from_csv('orders.csv'))
    ->filter(ref('amount')->isNotNull())
    ->write(to_json('orders.json'))
    ->run();
```

That is a complete program. The pipeline reads from top to bottom. It streams
rows in constant memory, for each input size.

## Mental model

Every Flow pipeline has three stages:

- Extract: `->read(...)` pulls rows from a source (file, API, database).
- Transform: `->filter()`, `->withColumn()`, `->join()`, `->groupBy()`,
  `->window()`, and friends shape the rows.
- Load: `->write(...)` streams the result into a loader. `->run()` executes
  the full pipeline.

Data moves through the pipeline as DataFrame → Rows batches of columns. Each `Rows`
batch carries the schema that types its columns.

## Where to go from here

New to Flow

- [Installation](/documentation/installation): composer setup and runtime tips.
- [Quick Start](/documentation/quick-start): from empty project to working
  pipeline in five minutes.

Learn the API

- [Data Frame](/documentation/components/core/core): the core: filter, join,
  group by, window, partition, sort, limit.
- [DSL Reference](/documentation/dsl/core): every function in the DSL, with
  signatures and examples.

Working with a specific source or destination

- Browse [Adapters](/documentation/components/adapters/csv): CSV, JSON, XML,
  Parquet, Excel, HTTP, PostgreSQL, Doctrine, SEAL, ChartJS.

See it running

- [Examples](/reading/array): runnable snippets grouped by
  topic and format. Open any example in the Playground.

Going to production

- [Schema](/documentation/components/core/schema),
  [Save Mode](/documentation/components/core/save-mode),
  [Caching](/documentation/components/core/caching),
  [Telemetry](/documentation/components/libs/telemetry).

## What Flow isn't

Flow is not a database engine, a query planner, or a distributed compute
framework. It runs inside each PHP runtime (CLI, FPM, queue worker) and
processes rows in constant memory. For cluster-scale joins or SQL planning,
use a distributed engine. For easy, type-safe data pipelines inside a PHP
application, use Flow.

## Get help

- Open an issue on [GitHub](https://github.com/flow-php/flow/issues).
- Drop in on [Discord](https://discord.gg/5dNXfQyACW).
- [Sponsor](/sponsor) to support development.
