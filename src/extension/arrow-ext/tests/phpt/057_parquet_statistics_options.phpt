--TEST--
STATISTICS_TRUNCATE_LENGTH reaches the arrow writer: a long string's column chunk statistics are truncated to it and marked inexact; both engines refuse 0 and negatives alike
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
--EXPECT--
default: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab false false
8: aaaaaaaa aaaaaaab false false
php 0: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, 0 given
arrow 0: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, 0 given
php -1: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, -1 given
arrow -1: Flow\Parquet\Exception\InvalidArgumentException: STATISTICS_TRUNCATE_LENGTH must be greater than 0, -1 given
