# Floe File Format

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Floe is Flow's native, self-describing, columnar binary file format. Each batch is written as one BATCH frame holding
every column in the Arrow buffer layout, the schema lives in the file's footer, and `from_floe()` reads the batches
back into the configured [column backend](/documentation/components/core/column-backend.md). Files use the `.floe`
extension.

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

Batches hold at most `withBatchSize()` rows, never spanning two BATCH frames.

The footer is the schema: `from_floe(...)->withSchema()` throws. Change a column's type or zone after reading:

```php
<?php

use function Flow\ETL\DSL\{data_frame, ref, to_output, to_timezone};
use function Flow\Floe\DSL\from_floe;

data_frame()
    ->read(from_floe(__DIR__ . '/output.floe'))
    ->withEntry('at', to_timezone(ref('at'), 'Europe/Warsaw'))
    ->write(to_output())
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

use function Flow\ETL\DSL\{append, data_frame, from_array};
use function Flow\Floe\DSL\{from_floe, to_floe};

data_frame()
    ->read(from_array([['id' => 3]]))
    ->write(to_floe(__DIR__ . '/data/dataset.floe')->saveMode(append()))
    ->run();

// reads dataset.floe plus every appended sibling
data_frame()
    ->read(from_floe(__DIR__ . '/data/*.floe'))
    ->run();
```

> Floe additionally supports appending into a **single** file through the low-level
> `Flow\Floe\FloeWriter::append()` API - appended batches must match the file's schema (a drifted
> batch throws `IncompatibleSchemaException`; schema evolution is only available through
> `merge_floe()`). Before the check, the appending schema is rewritten into the file's declared
> column and structure-field order, so a batch that carries the same columns or the same structure
> fields in a different order appends cleanly instead of throwing. The DataFrame `Append` save mode
> uses the sibling-file behavior for consistency with the rest of Flow.

## Partitioning

Partitioned datasets write one `.floe` file per partition directory. Reading prunes by path like
other file-based sources:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, partition_by, ref};
use function Flow\Floe\DSL\{from_floe, to_floe};

data_frame()
    ->read(from_array([
        ['id' => 1, 'country' => 'PL'],
        ['id' => 2, 'country' => 'US'],
    ]))
    ->write(to_floe(__DIR__ . '/data/dataset.floe')
        ->partitionBy(partition_by(ref('country'))))
    ->run();

// prune to a single partition
data_frame()
    ->read(from_floe(__DIR__ . '/data/country=PL/dataset.floe'))
    ->run();
```

## Limit & Offset Pushdown

`limit()` stops reading early, and `withOffset()` skips whole file sections using the footer before
reading, then passes over every BATCH frame whose row count lies inside the offset without decoding it, so
neither scans the full file:

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

use function Flow\Floe\DSL\from_floe;

$schema = from_floe(__DIR__ . '/output.floe')->schema();
```

## Reading the First or Last Rows

The low-level reader pulls just the head or tail of a single `.floe` file without a DataFrame. `tail()`
reads the row count from the footer and seeks past the leading sections, so it decodes only from the
boundary section onward - the leading rows are never read:

```php
<?php

use Flow\ETL\Column\AdaptiveBackend;
use Flow\Floe\FloeReader;
use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$file = (new FloeReader(native_local_filesystem(), new AdaptiveBackend()))->read(path(__DIR__ . '/output.floe'));

foreach ($file->head(300) as $rows) {
    // first 300 rows; stops reading once 300 are yielded
}

foreach ($file->tail(300) as $rows) {
    // last 300 rows; decoded from the boundary section onward
}
```

Both yield `Rows` in batches of at most 1000 rows (override with the second argument) and return every row when
the file holds fewer than the requested count. For the first N through the DataFrame API use `->limit(N)`
(pushed into the extractor); `tail()` is a reader-level convenience because it needs the file's total
from the footer.

## Merging Files

`merge_floe()` combines several `.floe` files (same or append-compatible evolving schema) into one.
The default byte-splices frame regions without re-encoding a single row - O(bytes); `compact: true`
re-reads and re-writes every batch, coalescing same-schema runs into fewer sections:

```php
<?php

use Flow\ETL\Column\AdaptiveBackend;

use function Flow\Floe\DSL\merge_floe;

merge_floe(
    [__DIR__ . '/data/part-1.floe', __DIR__ . '/data/part-2.floe'],
    __DIR__ . '/data/merged.floe',
    new AdaptiveBackend(),
);
```

Sources must evolve the running merged schema cleanly - otherwise `IncompatibleSchemaException` is
thrown before anything is written. `merge_floe()` works on the local filesystem; for other filesystems use `Flow\Floe\FloeMerger` directly.

## Whole-Value Serialization

The whole-value serialization paths - the [cache](/documentation/components/core/caching.md) and
`Flow\Floe\FloeSerializer` (the config default serializer) - stream: `serialize(Rows, DestinationStream)`
goes through `FloeStreamWriter`, which writes one BATCH frame, and `unserialize(SourceStream)` through the
strict `FloeStreamReader::rows()`, read in frame-sized batches of at most `batchSize` rows (default 1000) and
returned as one `Rows` in the serializer's backend. String payloads round-trip through the `Flow\Serializer\DSL`
helpers `serialize_to_string()` / `unserialize_from_string()`. A whole-value `unserialize()` verifies the decoded row
count against the footer and rejects torn payloads.

## Validation

Floe never casts. Values are checked when a batch is built - a value that does not fit its column throws
`SchemaMismatchException` before anything reaches the writer. The writer then checks each batch's schema against
the file's schema, fixed when the write session starts: `FloeWriter` takes it as an argument, `to_floe()` takes
`withSchema()` or else the first batch's schema:

| Batch schema                                                  | Result                        |
|---------------------------------------------------------------|-------------------------------|
| a column the file does not declare                            | `IncompatibleSchemaException` |
| lacks a column the file declares nullable                     | written, reads back as null   |
| lacks a column the file declares not nullable                 | `IncompatibleSchemaException` |
| a column of another type, or nullable where the file's is not | `IncompatibleSchemaException` |

```php
<?php

use Flow\ETL\Column\PhpBackend;
use Flow\Floe\FloeWriter;
use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$writer = new FloeWriter(native_local_filesystem(), schema(int_schema('id')), new PhpBackend());
$writer->create(path(__DIR__ . '/orders.floe'));
$writer->write(array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend()));
$writer->write(array_to_rows([['id' => 'AB-1']], schema(str_schema('id')), new PhpBackend()));
```

```
Floe write session schema is fixed and this batch does not fit it:   Mismatched Definitions:
    |-- expected: id<integer>, given: id<string>
```

The check runs before any bytes reach the destination, so a rejected batch leaves the file readable.

## On-Disk Layout

A `.floe` file is a 6-byte header, a stream of length-prefixed frames and a JSON footer located from the last
8 bytes. The schema lives only in the footer. Integers are little-endian.

```
HEADER                               6 bytes
FRAMES (write order)
  BATCH   (0x05)                     one per write()
  ...
FOOTER    (0x06)                     footer JSON + TRAILER (8 bytes)
```

### Header (6 bytes)

```
'F' 'L' 'O' 'E'  version  codec
                  0x02     0x00 = no compression
```

### Frame envelope

```
type     1 byte          0x05 BATCH   0x06 FOOTER
length   4 bytes (u32)
body     <length> bytes
```

Any other frame type is refused with `Floe found unknown frame type 0x%02X`.

### BATCH body

One batch in columnar form: a plaintext directory, then the buffer area.

```
rowCount     u32
nodeCount    u32
bufferCount  u32
nodes        nodeCount   × {length u32, nullCount u32}    pre-order, every column's type tree
extents      bufferCount × {offset u32, length u32}       into the buffer area
padding      to 8 bytes
buffer area  per non-empty buffer: i64 uncompressed length (-1 = stored raw) + bytes, padded to 8
```

- Nodes per type: scalar and null 1, list 1 + element, map 2 (map, entries) + key + value, structure 1 + children.
- Buffers per node: validity (`''` when the node holds no nulls), then offsets for list, map and string kinds,
  then values/data; a null column has none.
- An empty buffer is extent `{0, 0}` with no prefix.
- A codec compresses each buffer on its own; the buffer is stored raw (`-1`) whenever the codec output is not
  smaller. The directory is never compressed.

`[['id' => 1, 'name' => 'ab'], ['id' => 2, 'name' => null]]` under `int_schema('id')`,
`str_schema('name', nullable: true)` - 72-byte directory, 80-byte buffer area:

```
02000000 02000000 05000000                                         rows 2, nodes 2, buffers 5
02000000 00000000  02000000 01000000                               id {2,0}, name {2,1}
00000000 00000000  00000000 18000000  18000000 09000000            id validity {0,0}, id values {0,24}, name validity {24,9}
28000000 14000000  40000000 0a000000  00000000                     name offsets {40,20}, name data {64,10}, padding
ffffffffffffffff 0100000000000000 0200000000000000                 id values
ffffffffffffffff 01 00000000000000                                 name validity
ffffffffffffffff 000000000200000002000000 00000000                 name offsets
ffffffffffffffff 6162 000000000000                                 name data
```

### Footer

```json
{
  "version":    2,
  "writer":     "1.x-dev",
  "schema":     [ /* the file's single schema */ ],
  "sections":   [ { "offset": 6, "rowCount": 2 } ],
  "statistics": { "rows": 2, "byteSize": 157 },
  "metadata":   { /* Schema\Metadata */ }
}
```

- `sections` - byte `offset` and `rowCount` of each section, for offset/limit pushdown without a scan.
- `statistics` - row count and the bytes of the data frames (header and footer excluded).
- Unknown keys are ignored, so a field added later does not break an older reader.

### Trailer (last 8 bytes)

```
      ┌───────────────┬───────────────┐
      │ footer length │ magic "FLOE"  │
      │ 4 bytes (LE)  │ 4 bytes       │
      └───────────────┴───────────────┘
```

A reader seeks to `EOF − 8`, reads the trailer, verifies the trailing `FLOE` magic, then seeks back
`footer length` bytes to parse the footer JSON - two ranged reads, no body scan. This is what powers
`FloeExtractor::schema()` and offset/limit pushdown.
