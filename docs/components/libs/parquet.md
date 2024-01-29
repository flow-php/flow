# Parquet

- [⬅️️ Back](../../introduction.md)

## Installation

```
composer require flow-php/parquet
```

## What is Parquet 

Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. 
It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. 
Parquet is available in multiple languages including Java, C++, Python, etc... **Now also in PHP!**

## Columnar Storage

Parquet stores data in a columnar format, but what does it means? 

Row-based format:

-----------------
| ID | Name  | Age |
|----|-------|-----|
| 1  | Alice | 20  |
| 2  | Bob   | 25  |
| 3  | Carol | 30  |

Column-based format:
--------------------
| ID | 1 | 2 | 3 |
|----|---|---|---|
| Name | Alice | Bob | Carol |
| Age  | 20    | 25  | 30    |


This approach has several advantages:

- **Compression**: Since data is stored in columns, it is naturally compressed better.
- **I/O**: When querying a subset of columns, we can skip reading the other columns. This is especially useful when the columns are large.
- **Encoding**: Different encoding schemes can be used for different columns, depending on the data type and the distribution of values.
- **Data skipping**: When querying a subset of rows, we can skip reading the other rows. This is especially useful when the rows are large.
- **Reading selective columns**: When querying a subset of columns, we can skip reading the other columns. This is especially useful when the columns are large.

### Parquet File Structure

```
4-byte magic number "PAR1"
<Column 1 Chunk 1 + Column Metadata>
<Column 2 Chunk 1 + Column Metadata>
...
<Column N Chunk 1 + Column Metadata>
<Column 1 Chunk 2 + Column Metadata>
<Column 2 Chunk 2 + Column Metadata>
...
<Column N Chunk 2 + Column Metadata>
...
<Column 1 Chunk M + Column Metadata>
<Column 2 Chunk M + Column Metadata>
...
<Column N Chunk M + Column Metadata>
File Metadata
4-byte length in bytes of file metadata (little endian)
4-byte magic number "PAR1"
```

![Parquet File Structure](https://parquet.apache.org/images/FileLayout.gif)

## Reading Parquet Files

The first thing we need to do is to create a reader. 

```php
use Flow\Parquet\Reader;

$reader = new Reader();
```

The Reader accepts two arguments: 

- `$byteOrder` - by default set to `ByteOrder::LITTLE_ENDIAN`
- `$options` - a set of options that can be used to configure the reader.

All available options are described in [Option](/src/lib/parquet/src/Flow/Parquet/Option.php) enum. 

> Please be aware that not all options are affecting reader. 

### Reader Options

- `INT_96_AS_DATETIME` - default: `true` - if set to `true` then `INT96` values will be converted to `DateTime` objects. 

### Reading a file

Once we have reader we can read a file. 

```php
use Flow\Parquet\Reader;

$reader = new Reader();

$file = $reader->read('path/to/file.parquet');
$file = $reader->readStream(\fopen('path/to/file.parquet', 'rb'));
```

At this point, nothing is read yet. We just created a file object.

There are several things we can read from parquet file: 

- `ParquetFile::values(array $columns = [], ?int $limit = null, ?int $offset = null) : \Generator`
- `ParquetFile::metadata() : Metadata`
- `ParquetFile::schema() : Schema` - shortcut for `ParquetFile::metadata()->schema()`

### Reading the whole file: 

```php
use Flow\Parquet\Reader;

$reader = new Reader();

$file = $reader->read('path/to/file.parquet');
foreach ($file->values() as $row) {
    // do something with $row
}
```

### Reading selected columns

```php
use Flow\Parquet\Reader;

$reader = new Reader();

$file = $reader->read('path/to/file.parquet');
foreach ($file->values(["column_1", "column_2"]) as $row) {
    // do something with $row
}
```

### Pagination 

> [!NOTE]  
> Paginating over parquet file is a bit tricky, especially if we want to keep memory usage low.
> To achieve the best results, we will need to play a bit with Writer options (covered later).

```php
use Flow\Parquet\Reader;

$reader = new Reader();

$file = $reader->read('path/to/file.parquet');
foreach ($file->values(["column_1", "column_2"], limit: 100, offset: 1000) as $row) {
    // do something with $row
}
```

## Writing Parquet Files

Since parquet is a binary format, we need to provide a schema for the writer so it can know how
to encode values in specific columns. 

Here is how we can create a schema: 

```php

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

$schema = Schema::with(
    FlatColumn::int64('id'),
    FlatColumn::string('name'),
    FlatColumn::boolean('active'),
    FlatColumn::dateTime('created_at'),
    NestedColumn::list('list_of_int', Schema\ListElement::int32()),
    NestedColumn::map('map_of_int_string', Schema\MapKey::int32(), Schema\MapValue::string()),
    NestedColumn::struct('struct', [
        FlatColumn::int64('id'),
        FlatColumn::string('name'),
        FlatColumn::boolean('active'),
        FlatColumn::dateTime('created_at'),
        NestedColumn::list('list_of_int', Schema\ListElement::int32()),
        NestedColumn::map('map_of_int_string', Schema\MapKey::int32(), Schema\MapValue::string()),
    ])
);
```

Once we have a schema, we can create a writer. 

```php
use Flow\Parquet\Writer;

$writer = new Writer();
```

and write our data: 

```
$writer->write(
    $path, 
    $schema, 
    [
        [
            'id' => 1,
            'name' => 'Alice',
            ...
        ]
    ]
);
```

This approach will open a parquet file, create a group writer, write all data and close the file.
It requires to keep whole dataset in memory which usually is not the best approach. 

### Writing data in chunks

Before we can write a batch of rows, we need to open a file. 

```php
$writer->open($path, $schema);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->close();
```

We can also open a file for a resource:

```php
$writer->openForStream($resource, $schema);
```

### Writing a single row

```php
$writer->open($path, $schema);
$writer->writeRow($row);
$writer->writeRow($row);
$writer->writeRow($row);
$writer->writeRow($row);
$writer->writeRow($row);
$writer->close();
```

### Appending data to existing file

Like with writing to the file we can append entire dataset or batch or single row. 

```php
$writter->append($path, $rows); 
```

First we need to reopen a file or stream: 

```php
$writer->reopen($path);
$writer->reopenForStream(\fopen($path, 'rb+'));

$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
$writer->writeBatch([$row, $row]);
```

As we can see, we don't need to provide a schema as it is already stored in the file.

> [!WARNING]  
> At this point, schema evolution is not yet supported. 
> We need to make sure that schema is the same as the one used to create a file.

### Writer Options

- `BYTE_ARRAY_TO_STRING` - default: `true` - if set to `true` then `BYTE_ARRAY` values will be converted to `string` objects.
- `DICTIONARY_PAGE_MIN_CARDINALITY_RATION` - default '0.4' - minimum ratio of unique values to total values for a column to have dictionary encoding.
- `DICTIONARY_PAGE_SIZE` - default: `1Mb` - maximum size of dictionary page.
- `GZIP_COMPRESSION_LEVEL` - default: `9` - compression level for GZIP compression (applied only when GZIP compression is enabled).
- `PAGE_SIZE_BYTES` - default: `8Kb` - maximum size of data page.
- `ROUND_NANOSECONDS` - default: `false` - Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true, reader will round nanoseconds to microseconds.
- `ROW_GROUP_SIZE_BYTES` - default: `8Mb` - maximum size of row group. 
- `ROW_GROUP_SIZE_CHECK_INTERVAL` default: `1000` - number of rows to write before checking if row group size limit is reached.
- `VALIDATE_DATA` - default: `true` - if set to `true` then writer will validate data against schema.
- `WRITER_VERSION` - default `1` - tells writer which version of parquet format should be used.

Two most important options that can heavily affect memory usage are:

- `ROW_GROUP_SIZE_BYTES`
- `ROW_GROUP_SIZE_CHECK_INTERVAL`

Row Group Size defines pretty much how much data writer (but also reader) will need to keep in memory
before flushing it to the file.
Row group size check interval, defines how often writer will check if row group size limit is reached.
If you set this value too high, writer might exceed row group size limit. 

By default tools like Spark or Hive are using 128-512Mb as a row group size.
Which is great for big data, and quick processing in memory but not so great for PHP.

For example, if you need to paginate over file with 1Gb of data, and you set row group size to 512Mb,
you will need to keep at least 512Mb of data in memory at once. 

A Much better approach is to reduce the row group size to something closer to 1Mb, and row grpu size check interval to 
what your default page size should be - like for example 100 or 500 (that obviously depends on your data)

This way you will keep memory usage low, and you will be able to paginate over big files without any issues.
But it will take a bit longer to write into those files since writter will need to flush and calculate staticists
more frequently. 

Unfortunately, there is no one size fits all solution here. 
You will need to play a bit with those values to find the best one for your use case.

## Compressions

Parquet supports several compression algorithms.

 - `BROTLI` - not yet supported  
 - `GZIP` - supported out of the box 
 - `LZ4` - not yet supported 
 - `LZ4_RAW` - not yet supported
 - `LZO`  - not yet supported
 - `SNAPPY` - supported - it's recommended to install [Snappy Extension](https://github.com/kjdev/php-ext-snappy) - otherwise php implementation is used that is much slower than extension
 - `UNCOMPRESSED` - supported out of the box 
 - `ZSTD` - not yet supported

Obviously, compression is a trade-off between speed and size.
If you want to achieve the best compression, you should use `GZIP` or `SNAPPY` which is a default compression algorithm.

For not yet supported algorithms, please check our [Roadmap](https://github.com/orgs/flow-php/projects/1) to understand when they will be supported.

