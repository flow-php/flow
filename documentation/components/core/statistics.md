# Source Statistics

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Every extractor says what it knows about its data before a row is read - how many rows, how many bytes:

```php
<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;

$statistics = from_parquet('orders.parquet')->statistics();

$statistics->rows; // Flow\ETL\Values\Cardinality
$statistics->size; // Flow\ETL\Values\Cardinality, bytes
```

The answer describes the whole source, with no `limit()` or partition filter applied.

## Cardinality

A `Cardinality` carries a guarantee and a guess side by side:

```php
<?php

use Flow\ETL\Values\Cardinality;

Cardinality::exact(1_000);                          // atMost 1000, estimate 1000, relativeError 0.0
Cardinality::atMost(1_000);                         // atMost 1000 - a guaranteed upper bound, no estimate
Cardinality::approximately(1_000);                  // estimate 1000, relativeError 0.5 (DEFAULT_RELATIVE_ERROR)
Cardinality::unknown();                             // nothing known

Cardinality::exact(1_000)->exactly();               // 1000 - null unless the count is exact
Cardinality::approximately(1_000)->confident(0.25); // null - the estimate is looser than 25%
Cardinality::unknown()->isUnknown();                // true
```

`atMost` is never exceeded. `estimate` may be wrong in either direction, by roughly `relativeError`.

## What each source declares

| Source                                                              | rows                                                                                                                      | size                         |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `from_parquet()`, `from_floe()`                                     | exact from the footers; an estimate when a glob's footers were not all read                                               | exact, or estimate on a glob |
| `from_csv()`, `from_json_lines()`                                   | unknown before the schema is sniffed; exact when the sniff read every file whole; else an estimate from the mean row size | exact listed bytes           |
| `from_json()`, `from_excel()`                                       | unknown before the schema is sniffed; exact when the sniff read every file whole; else unknown                            | exact listed bytes           |
| `from_text()`, `from_xml()`                                         | unknown                                                                                                                   | exact listed bytes           |
| `files()`                                                           | exact listed file count                                                                                                   | exact listed bytes           |
| `from_path_partitions()`                                            | exact listed file count                                                                                                   | unknown                      |
| `from_array()`, `from_rows()`, `from_memory()`, `from_sequence_*()` | exact                                                                                                                     | unknown                      |
| `from_cache()`                                                      | exact from the cache index                                                                                                | unknown                      |
| `from_pgsql_*()`, `from_dbal_*()`                                   | estimate from `EXPLAIN` (never `ANALYZE`), bounded by `withMaximum()`                                                     | unknown                      |
| `from_google_sheet()`                                               | bounded by the sheet's grid size once the schema is sniffed                                                               | unknown                      |
| `from_all()`                                                        | what the wrapped extractors declare, merged                                                                               | same                         |
| `from_avro()`, `from_*_http_*()`                                    | unknown                                                                                                                   | unknown                      |

## Where statistics are used

- `explain()->toString(Stage::physical)` lists them under every source: `Statistics: rows exact 1 000 · size exact 327 527 B`.
- `count()` over a source with exact rows reads the number instead of the rows - see
  [Data Retrieval](/documentation/components/core/data-retrieval.md).
- `run(analyze: analyze()->withSourceStatistics())` puts the declared rows next to the rows each source yielded.

## A custom extractor

`statistics()` is part of the `Extractor` contract. A source that knows nothing returns `new Statistics()`:

```php
<?php

use Flow\ETL\Values\Cardinality;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Statistics;

final class ApiExtractor implements Extractor
{
    private ?Statistics $statistics = null;

    public function __construct(private readonly ApiClient $api)
    {
    }

    public function statistics(): Statistics
    {
        return $this->statistics ??= new Statistics(
            rows: Cardinality::approximately($this->api->totalHint(), Cardinality::DEFAULT_RELATIVE_ERROR),
        );
    }

    // extract(), schema(), withSchema()
}
```

`statistics()` is called at most once per run, only when something needs the answer. Memoise what it reads. It
may spend only what the run spends anyway - a listing it must produce, metadata of files `schema()` already opens -
and never open an extra file or make an extra remote call. A database source may ask its planner (`EXPLAIN`), never
run the query.

## Describing a source

A reader or a sniffer that can be rebuilt from plain data implements `Flow\ETL\Foundation\Describe\Describable`:

```php
<?php

use Flow\ETL\Foundation\Describe\{ConfiguredFilesystem, Describable, Description, Rebuild};
use Flow\Filesystem\Filesystem;

final readonly class ApiReader implements Describable
{
    public function __construct(private Filesystem $filesystem, private ApiOptions $options)
    {
    }

    public function describe(): ?Description
    {
        // null when the filesystem holds a live client or memory: compose() makes the whole description null
        return Description::compose(self::class, [
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
            'options' => $this->options->normalize(),
        ]);
    }

    public static function fromDescription(#[\SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['filesystem'], ConfiguredFilesystem::class)->filesystem(),
            ApiOptions::fromArray($data['options']),
        );
    }
}
```

- a library object is not `Describable`: it exposes `normalize(): ?array` / `fromArray(array)` and core wraps it in an
  adapter (`ConfiguredFilesystem`, `ConfiguredParquetEngine`); a filesystem's adapter is secret, so a description
  nesting one is `sensitive()` and may only cross a ciphered exchange;
- a value object travels as `normalize(): array` / `fromArray(array)`, enums as `{enum, case}`, paths as
  `Path::normalize()`; input is untrusted and validated on the way back in;
- `#[\SensitiveParameter]` on every constructor and `fromDescription()` argument that may hold a credential keeps it
  out of stack traces; `null` from `describe()` means "this instance stays in-process" and is never an error.
