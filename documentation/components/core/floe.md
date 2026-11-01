# Floe File Format

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Floe is the native columnar binary file format of Flow. A Floe file holds its own schema. Flow writes each batch as
one BATCH frame. The frame holds every column in the Arrow buffer layout. The schema is in the footer of the file.
`from_floe()` reads the batches back into the configured [column backend](/documentation/components/core/column-backend.md) of the DataFrame.
Files use the `.floe` extension.

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

A batch holds at most `withBatchSize()` rows. A batch never spans two BATCH frames.

The footer is the schema. `withSchema()` reads every file by column name against a schema that you declare, as
[Parquet](/documentation/components/adapters/parquet.md) does. To change the type or the time zone of a column, do it after the read:

```php
<?php

use function Flow\ETL\DSL\{data_frame, ref, to_output, to_timezone};
use function Flow\Floe\DSL\from_floe;

data_frame()
    ->read(from_floe(__DIR__ . '/output.floe'))
    ->withColumn('at', to_timezone(ref('at'), 'Europe/Warsaw'))
    ->write(to_output())
    ->run();
```

## Save Modes

`to_floe()` supports every [Save Mode](/documentation/components/core/save-mode.md) in the same way as other file
loaders:

- ExceptionIfExists (default): if the destination exists, the write throws.
- Overwrite: replaces the destination.
- Ignore: if the destination exists, the write is skipped.
- Append: writes a new sibling `.floe` file for each run, as every other file format does.
  A read of a directory of `.floe` files returns their union.

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

> Floe can also append to one file through the low-level `Flow\ETL\Storage\Floe\FloeFiles::append()` API.
> The appended batches must match the schema of the file. A batch with a different schema throws
> `IncompatibleSchemaException`. Only `merge_floe()` can change the schema. Before the check, Flow puts the columns
> and structure fields of the batch in the order that the file declares. Thus a batch with the same columns or
> structure fields in a different order appends without an error. The DataFrame `Append` save mode writes sibling files,
> as in the rest of Flow.

## Partitioning

A partitioned dataset has one `.floe` file in each partition directory. A read prunes by path, as other file sources
do:

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

`limit()` stops the read early. Before the read, `offset()` uses the footer to skip full file sections. Then it skips
each BATCH frame whose rows are all inside the offset, without decoding it. Thus neither of them scans the full file.
If the source takes a filter, the filter runs first. The offset then counts the rows that the filter keeps:

```php
<?php

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

data_frame()
    ->read(from_floe(__DIR__ . '/output.floe'))
    ->offset(1_000)
    ->limit(100)
    ->run();
```

## Source Schema Without a Scan

`FloeExtractor::schema()` reads only the file footers, with two ranged reads for each file. You get the schema of the
source without a scan of the rows:

```php
<?php

use function Flow\Floe\DSL\from_floe;

$schema = from_floe(__DIR__ . '/output.floe')->schema();
```

## Reading a Range of Rows

The low-level reader reads a range of one `.floe` file without a DataFrame. It uses the sections in the footer to
find the section that holds the first requested row. It decodes only from that section to the end of the range:

```php
<?php

use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Values\Column\AdaptiveBackend;
use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$file = (new FloeFiles(native_local_filesystem(), new AdaptiveBackend()))->read(path(__DIR__ . '/output.floe'));

foreach ($file->rows(batchSize: 1000, offset: 0, limit: 300) as $rows) {
    // the first 300 rows
}

foreach ($file->rows(batchSize: 1000, offset: max(0, $file->totalRows() - 300)) as $rows) {
    // the last 300 rows
}

$file->close();
```

## Merging Files

`merge_floe()` combines several `.floe` files into one. The files must have the same schema, or a schema that can
grow by append. By default, it copies the frame bytes and encodes no row again. The cost is O(bytes).
With `compact: true`, it reads and writes every batch again. It joins runs of batches with the same schema into fewer sections:

```php
<?php

use Flow\ETL\Values\Column\AdaptiveBackend;

use function Flow\Floe\DSL\merge_floe;

merge_floe(
    [__DIR__ . '/data/part-1.floe', __DIR__ . '/data/part-2.floe'],
    __DIR__ . '/data/merged.floe',
    new AdaptiveBackend(),
);
```

Each source must fit the merged schema so far, or extend it by append. If a source does not, Flow throws
`IncompatibleSchemaException` before it writes anything. `merge_floe()` works on the local filesystem. For other
filesystems, use `Flow\ETL\Storage\Floe\FloeMerger` directly.

## Whole-Value Serialization

Two paths serialize a whole value: the [cache](/documentation/components/core/caching.md) and
`Flow\ETL\Storage\Floe\FloeSerializer`. This serializer is the default serializer of the configuration. Both paths stream.
`serialize(Rows, DestinationStream)` goes through `FloeStreamWriter`, which writes one BATCH frame.
`unserialize(SourceStream)` goes through the strict `FloeStreamReader::rows()`. It reads batches of one frame each,
with at most `batchSize` rows (default 1000). It returns them as one `Rows` in the backend of the serializer.
String payloads go through the `Flow\Serializer\DSL` functions `serialize_to_string()` / `unserialize_from_string()`
and come back unchanged. A whole-value `unserialize()` compares the decoded row count with the footer. It rejects an incomplete payload.

## Validation

Floe never casts. When Flow builds a batch, it tests each value. A value that does not fit its column throws
`SchemaMismatchException` before anything goes to the writer. The writer then compares the schema of each batch with
the schema of the file. The write session fixes the schema of the file at its start. `FloeFiles::create()` takes it
as an argument. `to_floe()` takes `withSchema()`, or else the schema of the first batch:

| Batch schema                                                  | Result                        |
|---------------------------------------------------------------|-------------------------------|
| a column the file does not declare                            | `IncompatibleSchemaException` |
| lacks a column the file declares nullable                     | written, reads back as null   |
| lacks a column the file declares not nullable                 | `IncompatibleSchemaException` |
| a column of another type, or nullable where the file's is not | `IncompatibleSchemaException` |

```php
<?php

use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Values\Column\PhpBackend;
use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$writer = (new FloeFiles(native_local_filesystem(), new PhpBackend()))->create(path(__DIR__ . '/orders.floe'), schema(int_schema('id')));
$writer->write(array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend()));
$writer->write(array_to_rows([['id' => 'AB-1']], schema(str_schema('id')), new PhpBackend()));
```

```
Floe write session schema is fixed and this batch does not fit it:   Mismatched Definitions:
    |-- expected: id<integer>, given: id<string>
```

The test runs before any bytes go to the destination. Thus a rejected batch does not damage the file.

## On-Disk Layout

A `.floe` file is a 6-byte header, a stream of frames with a length prefix, and a JSON footer. The last 8 bytes
give the position of the footer. The schema is only in the footer. Integers are little-endian.

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

Any other frame type fails with `Floe found unknown frame type 0x%02X`.

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
- Buffers per node: validity first. When the node holds no nulls, validity is `''`. Then offsets for list, map and
  string kinds, then values/data. A null column has no buffers.
- An empty buffer is extent `{0, 0}` with no prefix.
- A codec compresses each buffer separately. If the codec output is not smaller, the buffer is stored raw (`-1`).
  The directory is never compressed.

`[['id' => 1, 'name' => 'ab'], ['id' => 2, 'name' => null]]` under `int_schema('id')`,
`str_schema('name', nullable: true)` gives a 72-byte directory and an 80-byte buffer area:

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

- `sections`: byte `offset` and `rowCount` of each section, for offset/limit pushdown without a scan.
- `statistics`: row count and the bytes of the data frames (header and footer excluded).
- A reader ignores unknown keys. Thus a field added later does not break an older reader.

### Trailer (last 8 bytes)

```
      ┌───────────────┬───────────────┐
      │ footer length │ magic "FLOE"  │
      │ 4 bytes (LE)  │ 4 bytes       │
      └───────────────┴───────────────┘
```

A reader seeks to `EOF − 8` and reads the trailer. It makes sure that the trailer ends with the `FLOE` magic. Then
it seeks back `footer length` bytes and parses the footer JSON. This takes two ranged reads and no scan of the body.
`FloeExtractor::schema()` and offset/limit pushdown use this.
