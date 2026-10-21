--TEST--
COLUMNS_COMPRESSIONS, COLUMNS_ENCODINGS and COLUMNS_BLOOM_FILTERS reach the arrow writer by column path: a digit-only path (an int array key) applies to its column, a dictionary encoding enables the column's dictionary, any other encoding is the column's only encoding, as the PHP writer writes them, an encoding the column type cannot take is refused with the PHP writer's message, a key that is not UTF-8 or a value that is not a string is refused instead of crashing
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;

use function Flow\Filesystem\DSL\path;

$write = static fn(Option $option, array $columns): string => MemoryParquetFile::written(
    new Writer(
        options: (new Options())->set($option, $columns),
        engine: new RustParquetEngine(),
    ),
    Schema::with(FlatColumn::int64('2024'), FlatColumn::int64('other')),
    [['2024' => 1, 'other' => 2], ['2024' => 3, 'other' => 4]],
);

foreach (MemoryParquetFile::read(Reader::php(), $write(Option::COLUMNS_COMPRESSIONS, ['2024' => Compressions::GZIP]))->metadata()->columnChunks() as $chunk) {
    echo $chunk->flatPath(), ': ', $chunk->codec()->name, "\n";
}

foreach (MemoryParquetFile::read(Reader::php(), $write(Option::COLUMNS_ENCODINGS, ['2024' => Encodings::DELTA_BINARY_PACKED]))->metadata()->columnChunks() as $chunk) {
    echo $chunk->flatPath(), ': ', in_array(Encodings::DELTA_BINARY_PACKED, $chunk->encodings(), true) ? 'DELTA_BINARY_PACKED' : 'default encoding', "\n";
}

foreach (['php' => static fn(Options $options): Writer => Writer::php(options: $options), 'arrow' => static fn(Options $options): Writer => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
    $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
        $writer((new Options())->set(Option::COLUMNS_ENCODINGS, ['word' => Encodings::RLE_DICTIONARY])),
        Schema::with(FlatColumn::string('word')),
        [['word' => 'a'], ['word' => 'b'], ['word' => 'a']],
    ));

    foreach ($file->pageHeaders() as $page) {
        echo $engine, ': ', $page->pageHeader->type()->name, ' ', $page->pageHeader->encoding()->name, "\n";
    }
}

foreach (['php' => static fn(Options $options): Writer => Writer::php(options: $options), 'arrow' => static fn(Options $options): Writer => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
    $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
        $writer((new Options())->set(Option::COLUMNS_ENCODINGS, ['word' => Encodings::DELTA_BYTE_ARRAY])),
        Schema::with(FlatColumn::string('word')),
        [['word' => 'a'], ['word' => 'b'], ['word' => 'a']],
    ));

    foreach ($file->pageHeaders() as $page) {
        echo $engine, ': ', $page->pageHeader->type()->name, ' ', $page->pageHeader->encoding()->name, "\n";
    }
}

foreach (MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
    new Writer(options: (new Options())->set(Option::COLUMNS_ENCODINGS, ['word' => Encodings::PLAIN_DICTIONARY]), engine: new RustParquetEngine()),
    Schema::with(FlatColumn::string('word')),
    [['word' => 'a']],
))->pageHeaders() as $page) {
    echo 'arrow PLAIN_DICTIONARY: ', $page->pageHeader->type()->name, ' ', $page->pageHeader->encoding()->name, "\n";
}

foreach ([
    'flag' => [FlatColumn::boolean('flag'), Encodings::RLE_DICTIONARY, true],
    'fixed' => [FlatColumn::fixedSizeByteArray('fixed', 4), Encodings::RLE_DICTIONARY, 'abcd'],
    'amount' => [FlatColumn::double('amount'), Encodings::DELTA_BINARY_PACKED, 1.5],
] as $name => [$column, $encoding, $value]) {
    foreach (['php' => static fn(Options $options): Writer => Writer::php(options: $options), 'arrow' => static fn(Options $options): Writer => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
        echo $engine, ' ', $name, ': ', arrow_outcome(static fn() => MemoryParquetFile::written(
            $writer((new Options())->set(Option::COLUMNS_ENCODINGS, [$name => $encoding])),
            Schema::with($column),
            [[$name => $value]],
        )), "\n";
    }
}

echo arrow_outcome(static fn() => new RustParquetFileWriter(new StringDestinationStream(path('memory://file.parquet')), SchemaConverter::toExtension(Schema::with(FlatColumn::int64('id'))), Compressions::SNAPPY, ['COLUMNS_COMPRESSIONS' => ['id' => 1]], 100)), "\n";
echo arrow_outcome(static fn() => new RustParquetFileWriter(new StringDestinationStream(path('memory://file.parquet')), SchemaConverter::toExtension(Schema::with(FlatColumn::int64('id'))), Compressions::SNAPPY, ['COLUMNS_ENCODINGS' => ['id' => null]], 100)), "\n";
echo arrow_outcome(static fn() => $write(Option::COLUMNS_COMPRESSIONS, ["\xFF" => Compressions::GZIP])), "\n";
echo arrow_outcome(static fn() => $write(Option::COLUMNS_ENCODINGS, ["\xFF" => Encodings::PLAIN])), "\n";
echo arrow_outcome(static fn() => $write(Option::COLUMNS_BLOOM_FILTERS, ["\xFF" => new BloomFilterOptions()])), "\n";
?>
--EXPECT--
2024: GZIP
other: SNAPPY
2024: DELTA_BINARY_PACKED
other: default encoding
php: DICTIONARY_PAGE PLAIN
php: DATA_PAGE RLE_DICTIONARY
arrow: DICTIONARY_PAGE PLAIN
arrow: DATA_PAGE RLE_DICTIONARY
php: DATA_PAGE DELTA_BYTE_ARRAY
arrow: DATA_PAGE DELTA_BYTE_ARRAY
arrow PLAIN_DICTIONARY: DICTIONARY_PAGE PLAIN
arrow PLAIN_DICTIONARY: DATA_PAGE RLE_DICTIONARY
php flag: Flow\Parquet\Exception\InvalidArgumentException: RLE_DICTIONARY encoding is not supported for BOOLEAN columns. Column 'flag' has type: BOOLEAN
arrow flag: Flow\Parquet\Exception\InvalidArgumentException: RLE_DICTIONARY encoding is not supported for BOOLEAN columns. Column 'flag' has type: BOOLEAN
php fixed: Flow\Parquet\Exception\InvalidArgumentException: RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1. Column 'fixed' has type: FIXED_LEN_BYTE_ARRAY
arrow fixed: Flow\Parquet\Exception\InvalidArgumentException: RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1. Column 'fixed' has type: FIXED_LEN_BYTE_ARRAY
php amount: Flow\Parquet\Exception\InvalidArgumentException: DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. Column 'amount' has type: DOUBLE
arrow amount: Flow\Parquet\Exception\InvalidArgumentException: DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. Column 'amount' has type: DOUBLE
Flow\Parquet\Exception\InvalidArgumentException: COLUMNS_COMPRESSIONS['id'] must be a string
Flow\Parquet\Exception\InvalidArgumentException: COLUMNS_ENCODINGS['id'] must be a string
Flow\Parquet\Exception\InvalidArgumentException: COLUMNS_COMPRESSIONS has a key that is not a column path
Flow\Parquet\Exception\InvalidArgumentException: COLUMNS_ENCODINGS has a key that is not a column path
Flow\Parquet\Exception\InvalidArgumentException: COLUMNS_BLOOM_FILTERS has a key that is not a column path
