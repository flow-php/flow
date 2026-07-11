# Floe File Format

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Floe is Flow's native, self-describing binary file format for `Rows`. It stores the schema inside
the file, evolves seamlessly across appended sections, and reads back through the DataFrame API with
`from_floe()` / `to_floe()`. Files use the `.floe` extension.

## Writing

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};
use function Flow\Floe\DSL\to_floe;

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Jane'],
    ]))
    ->write(to_floe(__DIR__ . '/output.floe'))
    ->run();
```

## Reading

```php
<?php

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

data_frame()
    ->read(from_floe(__DIR__ . '/output.floe'))
    ->run();
```

## Save Modes

`to_floe()` honors every [Save Mode](/documentation/components/core/save-mode.md) through the same
machinery as other file loaders:

- **ExceptionIfExists** (default) - throws if the destination exists.
- **Overwrite** - replaces the destination.
- **Ignore** - skips writing when the destination exists.
- **Append** - writes a **new sibling `.floe` file** per run (like every other file format).
  Reading a directory of `.floe` files returns their union.

```php
<?php

use Flow\ETL\Filesystem\SaveMode;
use function Flow\ETL\DSL\{data_frame, from_array};
use function Flow\Floe\DSL\{from_floe, to_floe};

data_frame()
    ->read(from_array([['id' => 3]]))
    ->mode(SaveMode::Append)
    ->write(to_floe(__DIR__ . '/data/dataset.floe'))
    ->run();

// reads dataset.floe plus every appended sibling
data_frame()
    ->read(from_floe(__DIR__ . '/data/*.floe'))
    ->run();
```

> Floe additionally supports appending seamlessly into a **single** file with schema evolution
> through the low-level `Flow\Floe\FloeWriter::append()` API; the DataFrame `Append` save mode uses
> the sibling-file behavior for consistency with the rest of Flow.

## Partitioning

Partitioned datasets write one `.floe` file per partition directory. Reading prunes by path like
other file-based sources:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};
use function Flow\Floe\DSL\{from_floe, to_floe};

data_frame()
    ->read(from_array([
        ['id' => 1, 'country' => 'PL'],
        ['id' => 2, 'country' => 'US'],
    ]))
    ->partitionBy('country')
    ->write(to_floe(__DIR__ . '/data/dataset.floe'))
    ->run();

// prune to a single partition
data_frame()
    ->read(from_floe(__DIR__ . '/data/country=PL/dataset.floe'))
    ->run();
```

## Limit & Offset Pushdown

`limit()` stops reading early, and `withOffset()` skips whole file sections using the footer before
reading, so neither scans the full file:

```php
<?php

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

data_frame()
    ->read(from_floe(__DIR__ . '/output.floe')->withOffset(1_000))
    ->limit(100)
    ->run();
```

## Source Schema Without a Scan

`FloeExtractor::schema()` reads only the file footers (two ranged reads per file), so the source
schema is available without scanning any rows:

```php
<?php

use function Flow\ETL\DSL\{data_frame, flow_context, config};
use function Flow\Floe\DSL\from_floe;

$schema = from_floe(__DIR__ . '/output.floe')->schema(flow_context(config()));
```

## Reading the First or Last Rows

The low-level reader pulls just the head or tail of a single `.floe` file without a DataFrame. `tail()`
reads the row count from the footer and seeks past the leading sections, so it decodes only from the
boundary section onward — the leading rows are never read:

```php
<?php

use Flow\Floe\FloeReader;
use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$file = (new FloeReader(native_local_filesystem()))->read(path(__DIR__ . '/output.floe'));

foreach ($file->head(300) as $rows) {
    // first 300 rows; stops reading once 300 are yielded
}

foreach ($file->tail(300) as $rows) {
    // last 300 rows; decoded from the boundary section onward
}
```

Both yield `Rows` in batches (default 1000, override with the second argument) and return every row when
the file holds fewer than the requested count. For the first N through the DataFrame API use `->limit(N)`
(pushed into the extractor); `tail()` is a reader-level convenience because it needs the file's total
from the footer.

## Merging Files

`merge_floe()` combines several `.floe` files (same or append-compatible evolving schema) into one.
The default byte-splices frame regions without re-encoding a single row — O(bytes); `compact: true`
re-encodes every row, coalescing same-schema runs into fewer sections:

```php
<?php

use function Flow\Floe\DSL\merge_floe;

merge_floe(
    [__DIR__ . '/data/part-1.floe', __DIR__ . '/data/part-2.floe'],
    __DIR__ . '/data/merged.floe',
);
```

All sources must share one partition combination and evolve the running merged schema cleanly —
otherwise `IncompatibleSchemaException` is thrown before anything is written. `merge_floe()` works on
the local filesystem; for other filesystems use `Flow\Floe\FloeMerger` directly.

## Whole-Value Serialization

The whole-value serialization paths — the [cache](/documentation/components/core/caching.md) and
`Flow\Floe\FloeSerializer` (the config default serializer) — stream: encode goes through
`FloeWriter` and decode through `FloeFile::recover()` in batches of `batchSize` rows (default
1000), so the engine holds one batch at a time. The batch size never changes the produced bytes —
only the memory bound. A whole-value decode verifies the recovered row count against the footer
and rejects torn payloads. Numbers and guidance live in the
[caching documentation](/documentation/components/core/caching.md).

## On-Disk Layout

A `.floe` file is a fixed 6-byte header, a stream of length-prefixed frames, and a JSON footer that a
reader can locate from the last 8 bytes without scanning the body. All multi-byte integers are
**little-endian**.

```
┌──────────────────────────────────────────────────────────┐
│ HEADER                              6 bytes              │
├──────────────────────────────────────────────────────────┤
│ FRAMES  (repeated, in write order)                       │
│    PARTITIONS  (0x03)   once, partitioned files only     │
│    SCHEMA      (0x01)   emitted when the schema changes  │
│    ROW         (0x02)   one frame per row                │
│    ROW         (0x02)                                    │
│    …                                                     │
├──────────────────────────────────────────────────────────┤
│ FOOTER FRAME   (0x06)                                    │
│    footer JSON  +  TRAILER (8 bytes)                     │
└──────────────────────────────────────────────────────────┘
```

### Header (6 bytes)

```
 byte  0    1    2    3     4        5
      ┌────┬────┬────┬────┬────────┬────────┐
      │ 'F'│ 'L'│ 'O'│ 'E'│ version│ flags  │
      └────┴────┴────┴────┴────────┴────────┘
        magic "FLOE"        0x01     codec id
                                     (0x00 = no compression)
```

### Frame envelope

Every frame — `SCHEMA`, `ROW`, `PARTITIONS`, `FOOTER` — shares the same envelope: a 1-byte type, a
4-byte little-endian body length, then the body.

```
      ┌────────┬───────────────┬───────────────────────┐
      │ type   │ body length   │ body                  │
      │ 1 byte │ 4 bytes (LE)  │ <length> bytes        │
      └────────┴───────────────┴───────────────────────┘
        0x01 SCHEMA   0x02 ROW   0x03 PARTITIONS   0x06 FOOTER
```

- **SCHEMA (`0x01`)** — body is the JSON schema for the section that follows. A section's schema is
  **grow-only**: it starts as the first row's schema and a new `SCHEMA` frame is written only when a row
  introduces a new column or an incompatible type. Rows narrower than the section ride it (see the ROW
  absent flag), so a heterogeneous stream needs far fewer sections than one frame per distinct shape.
- **ROW (`0x02`)** — one row encoded **by column** against the current section schema: for each section
  column, in order, a one-byte presence flag — `0x01` present (followed by the value encoded per the
  column's schema type, no per-value tag), `0x00` null, `0x02` null-from-null, or `0x03` **absent** (the
  row has no such column). Only dynamically typed values (mixed/union columns, dynamic map keys) carry a
  one-byte type tag (`NULL`, `INTEGER`, `FLOAT`, `BOOLEAN`, `STRING`, `ARRAY`, `DATETIME`, `UUID`,
  `JSON`). A row whose columns exactly match the section produces the same bytes as a plain positional
  encode. One frame per row.
- **PARTITIONS (`0x03`)** — for a partitioned file, the partition key/value pairs, written once before
  the first section: a 4-byte count followed by repeated `[nameLen(4), name, valueLen(4), value]`.
- **FOOTER (`0x06`)** — the footer JSON followed by the trailer (below).

### Footer

The `FOOTER` frame body is a JSON object carrying everything needed to read the file **without
scanning rows**:

```json
{
  "version":    1,
  "writer":     "1.x-dev",
  "schemas":    [ /* schema body per schemaId */ ],
  "fileSchema": { /* merged schema of every section */ },
  "sections":   [ { "offset": 6, "schemaId": 0, "rowCount": 2 } ],
  "partitions": { "country": "PL" },
  "totalRows":  2,
  "metadata":   { /* typed key/value, Schema\Metadata */ }
}
```

- **`sections`** map a byte `offset` → `schemaId` + `rowCount`, so a reader can skip whole sections
  (offset/limit pushdown) and know each section's schema up front.
- **`schemas`** is the deduplicated list of every schema written; **`fileSchema`** is their merge —
  the source schema, available from the footer alone. On a seamless read, a row narrower than
  `fileSchema` (an absent column, or a column added by a later section) is padded with null entries so
  every yielded row conforms; `recover()` reads rows exactly as written, absent columns omitted.
- A new column or incompatible type grows the section (new `SCHEMA` frame); this is also how a single
  file evolves its schema across appended sections.

### Trailer (last 8 bytes)

```
      ┌───────────────┬───────────────┐
      │ footer length │ magic "FLOE"  │
      │ 4 bytes (LE)  │ 4 bytes       │
      └───────────────┴───────────────┘
```

A reader seeks to `EOF − 8`, reads the trailer, verifies the trailing `FLOE` magic, then seeks back
`footer length` bytes to parse the footer JSON — two ranged reads, no body scan. This is what powers
`FloeExtractor::schema()` and offset/limit pushdown.
