--TEST--
STATISTICS_TRUNCATE_LENGTH reaches the arrow writer: a long string's column chunk statistics are truncated to it and marked inexact; both engines refuse 0 and negatives alike. STATISTICS_LEVEL, COLUMN_INDEX_TRUNCATE_LENGTH and PAGE_HEADER_STATISTICS reach it too
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;
use Flow\Parquet\Writer\StatisticsLevel;

$column = FlatColumn::string('s');

foreach ([null, 8] as $length) {
    $options = $length === null ? new Options() : (new Options())->set(Option::STATISTICS_TRUNCATE_LENGTH, $length);
    $bytes = MemoryParquetFile::written(
        new Writer(options: $options, engine: new RustParquetEngine()),
        Schema::with($column),
        [['s' => str_repeat('a', 101)]],
    );
    $statistics = MemoryParquetFile::read(Reader::php(), $bytes)->metadata()->columnChunks()[0]->statistics();

    echo $length ?? 'default', ': ',
        $statistics->minValue($column), ' ', $statistics->maxValue($column), ' ',
        var_export($statistics->isMinValueExact(), true), ' ', var_export($statistics->isMaxValueExact(), true), "\n";
}
foreach ([0, -1] as $length) {
    foreach (['php' => Writer::php(options: (new Options())->set(Option::STATISTICS_TRUNCATE_LENGTH, $length)), 'arrow' => new Writer(options: (new Options())->set(Option::STATISTICS_TRUNCATE_LENGTH, $length), engine: new RustParquetEngine())] as $engine => $writer) {
        echo $engine, ' ', $length, ': ', arrow_outcome(static fn() => MemoryParquetFile::written($writer, Schema::with($column), [['s' => 'a']])), "\n";
    }
}
foreach (StatisticsLevel::cases() as $level) {
    $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
        new Writer(options: (new Options())->set(Option::STATISTICS_LEVEL, $level), engine: new RustParquetEngine()),
        Schema::with($column),
        [['s' => 'a']],
    ));

    echo $level->name, ': chunk ', var_export($file->metadata()->columnChunks()[0]->statistics() !== null, true),
        ', column index ', var_export($file->indexes()->columnIndex(0, $column) !== null, true),
        ', offset index ', var_export($file->indexes()->offsetIndex(0, $column) !== null, true), "\n";
}
$index = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
    new Writer(options: (new Options())->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 8), engine: new RustParquetEngine()),
    Schema::with($column),
    [['s' => str_repeat('a', 101)]],
))->indexes()->columnIndex(0, $column);
echo 'column index 8: ', $index->minValues[0], ' ', $index->maxValues[0], "\n";
foreach (['php' => Writer::php(options: (new Options())->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 0)), 'arrow' => new Writer(options: (new Options())->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 0), engine: new RustParquetEngine())] as $engine => $writer) {
    echo $engine, ' column index 0: ', arrow_outcome(static fn() => MemoryParquetFile::written($writer, Schema::with($column), [['s' => 'a']])), "\n";
}
foreach ([false, true] as $enabled) {
    $headers = iterator_to_array(MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
        new Writer(options: (new Options())->set(Option::PAGE_HEADER_STATISTICS, $enabled), engine: new RustParquetEngine()),
        Schema::with($column),
        [['s' => 'a']],
    ))->pageHeaders(), false);

    echo 'page header statistics ', var_export($enabled, true), ': ',
        var_export(end($headers)->pageHeader->dataPageHeader()?->statistics()?->maxValue($column), true), "\n";
}
--EXPECT--
default: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab false false
8: aaaaaaaa aaaaaaab false false
php 0: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, 0 given
arrow 0: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, 0 given
php -1: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, -1 given
arrow -1: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, -1 given
CHUNK: chunk true, column index false, offset index true
NONE: chunk false, column index false, offset index true
PAGE: chunk true, column index true, offset index true
column index 8: aaaaaaaa aaaaaaab
php column index 0: Flow\Parquet\Exception\InvalidArgumentException: COLUMN_INDEX_TRUNCATE_LENGTH must be greater than 0, 0 given
arrow column index 0: Flow\Parquet\Exception\InvalidArgumentException: COLUMN_INDEX_TRUNCATE_LENGTH must be greater than 0, 0 given
page header statistics false: NULL
page header statistics true: 'a'
