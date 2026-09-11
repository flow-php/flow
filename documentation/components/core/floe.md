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

When the [`flow_php` extension](/documentation/components/extensions/flow-php-ext.md) is loaded, the
strict read and the write fuse frame-split + value decode/encode + hydrate/dehydrate into one native
call per batch. It is transparent - the on-disk format is unchanged and the rows are byte-for-byte
identical to the pure-PHP engine.

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
boundary section onward - the leading rows are never read:

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
The default byte-splices frame regions without re-encoding a single row - O(bytes); `compact: true`
re-encodes every row, coalescing same-schema runs into fewer sections:

```php
<?php

use function Flow\Floe\DSL\merge_floe;

merge_floe(
    [__DIR__ . '/data/part-1.floe', __DIR__ . '/data/part-2.floe'],
    __DIR__ . '/data/merged.floe',
);
```

Sources must evolve the running merged schema cleanly - otherwise `IncompatibleSchemaException` is
thrown before anything is written. Each source's per-section partition combinations are preserved
(deduped into the merged footer table), so sources with differing combinations merge without error.
`merge_floe()` works on the local filesystem; for other filesystems use `Flow\Floe\FloeMerger` directly.

## Whole-Value Serialization

The whole-value serialization paths - the [cache](/documentation/components/core/caching.md) and
`Flow\Floe\FloeSerializer` (the config default serializer) - stream: `serialize(Rows, DestinationStream)`
goes through `FloeStreamWriter` and `unserialize(SourceStream)` through the strict `FloeStreamReader::rows()`
read in batches of `batchSize` rows (default 1000), so the engine holds one batch at a time. The batch size
never changes the produced bytes - only the memory bound. String payloads round-trip through the
`Flow\Serializer\DSL` helpers `serialize_to_string()` / `unserialize_from_string()`. A whole-value
`unserialize()` verifies the decoded row count against the footer and rejects torn payloads. Numbers and
guidance live in the [caching documentation](/documentation/components/core/caching.md).

## Validation

Floe asserts, it never casts. A value that does not match its column type is rejected - it is never
converted to fit.

Two checks run when a batch is written:

- **Column set** - a row carrying a column the file's schema does not declare is rejected. Always on.
- **Per value** - every value must satisfy its column type. Gated by `validateData`, on by default.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};
use function Flow\Floe\DSL\to_floe;

data_frame()
    ->read(from_array([['id' => 1], ['id' => 'AB-1']]))
    ->write(to_floe(__DIR__ . '/orders.floe'))
    ->run();
```

The first batch fixes the file's schema to `integer`, so the second one throws:

```
Floe write session schema is fixed and this batch does not fit it: column "id" (row 0):
could not convert 'AB-1' (string) to integer.
```

Both checks throw `IncompatibleSchemaException` before any bytes reach the destination, so a rejected
batch leaves the file readable. Other messages take the same shape:

```
column "amount" (row 0): could not convert null to string, column is not nullable
new column "email"
```

`validate_data: false` skips the per-value check only. The column-set check still runs, so silent
column loss cannot be unlocked. This mirrors parquet-java's `ParquetWriter::withValidation()`.

Two behaviours differ from other columnar formats on purpose:

- **Absent is not null.** A row that omits a column writes an absent flag and reads back as null;
  a row that carries an explicit `null` in a non-nullable column is rejected. Parquet turns a missing
  optional field into a null and rejects a missing required one; Arrow cannot omit a column at all.
- **An int is not a float.** An integer value in a `float` column is rejected. pyarrow, parquet-java
  and Avro all widen it silently.

## Unsupported column types

Floe stores one type per column, so a column whose type is only known per value cannot be written.
These throw `FloeException` when the write session opens:

```
Floe does not support values of type "mixed"
Floe does not support map keys of type "uuid"
Floe does not support structures that allow extra values
```

That covers `mixed`, union columns built with `union_schema()`, and `type_structure(..., allow_extra: true)`.
Use a declared element type, or a `json_schema()` column when the shape is genuinely dynamic.

## On-Disk Layout

A `.floe` file is a fixed 6-byte header, a stream of length-prefixed frames, and a JSON footer that a
reader can locate from the last 8 bytes without scanning the body. The schema lives **only in the
footer** - there is no inline schema frame. All multi-byte integers are **little-endian**.

```
┌──────────────────────────────────────────────────────────┐
│ HEADER                              6 bytes              │
├──────────────────────────────────────────────────────────┤
│ FRAMES  (repeated, in write order)                       │
│    PARTITIONS  (0x03)   emitted when combination changes │
│    ROW         (0x02)   one frame per row                │
│    ROW         (0x02)                                    │
│    ...                                                     │
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
        magic "FLOE"        0x02     codec id
                                     (0x00 = no compression)
```

### Frame envelope

Every frame - `ROW`, `PARTITIONS`, `FOOTER` - shares the same envelope: a 1-byte type, a
4-byte little-endian body length, then the body.

```
      ┌────────┬───────────────┬───────────────────────┐
      │ type   │ body length   │ body                  │
      │ 1 byte │ 4 bytes (LE)  │ <length> bytes        │
      └────────┴───────────────┴───────────────────────┘
        0x02 ROW   0x03 PARTITIONS   0x06 FOOTER
```

The file's schema is not a frame - it carries **exactly one schema**, fixed at session start (explicit,
or the first batch's union) and stored only in the footer (see `schema` below). A row narrower than the
schema rides it (see the ROW absent flag). A later batch that introduces a new column or an incompatible
type throws `IncompatibleSchemaException` - schema evolution lives only in `merge_floe()`.

- **ROW (`0x02`)** - one row encoded **by column** against the file schema: for each schema
  column, in order, a one-byte presence flag -
  - `0x01` **present** - followed by the value encoded per the column's schema type, no per-value tag.
  - `0x00` **null** - the value is null.
  - `0x02` **null with metadata** - a null value carrying per-value metadata that diverges from the
    column's schema metadata: a metadata block (4-byte little-endian JSON length + JSON), no value.
  - `0x03` **absent** - the row has no such column.
  - `0x04` **present with metadata** - a divergent-metadata block (4-byte length + JSON) followed by
    the encoded value.

  The two metadata flags (`0x02`, `0x04`) appear only when a row's per-value metadata differs from the
  column's schema metadata; otherwise every value uses `0x00`/`0x01`. Every value is encoded positionally
  against its column type, so no value carries a type tag. A row whose columns exactly match the section,
  with no divergent metadata, produces the same bytes as a plain positional encode. One frame per row.

  A **structure** value nests the same idea one level down: one flag byte per declared element, in the
  structure type's declared field order, with no element names on the wire -

  - `0x01` **present** - followed by the element value encoded per its declared type.
  - `0x00` **null** - the element key is present in the value, holding null.
  - `0x03` **absent** - the value has no such key (an `optional` element that was omitted).

  Because the bytes carry no names, the declared field order binds each flag byte to its element. The
  writer looks each element up **by name** in the value and emits in declared order, so a value whose
  keys arrive in a different order still produces the declared-order bytes; the reader rebuilds keys
  in declared order.
- **PARTITIONS (`0x03`)** - the partition key/value pairs for the section that follows: a 4-byte count
  followed by repeated `[nameLen(4), name, valueLen(4), value]`. Written at the start of every section
  whose combination differs from the previous one; the reader starts at the empty combination, so an
  unpartitioned first section emits none and a later change back to unpartitioned emits a `count=0`
  frame.
- **FOOTER (`0x06`)** - the footer JSON followed by the trailer (below).

### Footer

The `FOOTER` frame body is a JSON object carrying everything needed to read the file **without
scanning rows**:

```json
{
  "version":    2,
  "writer":     "1.x-dev",
  "schema":     { /* the file's single schema */ },
  "sections":   [ { "offset": 6, "partitionsId": 0, "rowCount": 2 } ],
  "partitions": [ { "country": "PL" } ],
  "totalRows":  2,
  "metadata":   { /* typed key/value, Schema\Metadata */ }
}
```

- **`sections`** map a byte `offset` → `partitionsId` + `rowCount`, so a reader can skip whole sections
  (offset/limit pushdown) and know each section's partition combination up front. Sections bound
  partition combinations and appends only; every section shares the file's one schema.
- **`partitions`** is the deduplicated, order-preserving table of partition combinations, indexed by
  `partitionsId`; the unpartitioned combination is an empty object at its own id. A file can hold many
  combinations (one per section).
- **`schema`** is the file's single schema and the sole place it is stored, so every read decodes
  against it without scanning the body. On a read, a row narrower than it (an absent column) is padded
  with null entries so every yielded row conforms. `merge_floe()` re-encodes drifted sources to their
  union, so a merged file is still one schema.

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
