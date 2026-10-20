--TEST--
PAGE_MAXIMUM_ROWS_COUNT reaches the arrow writer: its pages close at that many rows, as the PHP writer's do, whatever ARROW_WRITE_BATCH_SIZE; both engines refuse 0 and negatives alike
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;

$id = FlatColumn::int64('id');
// a null makes arrow-rs write in mini-batches of write_batch_size rows, not of the page row limit
$rows = array_map(static fn(int $id): array => ['id' => $id % 4 === 0 ? null : $id], range(1, 10));
$options = (new Options())
    ->set(Option::PAGE_MAXIMUM_ROWS_COUNT, 3)
    ->set(Option::PAGE_SIZE_CHECK_INTERVAL, 1);

foreach (['php' => Writer::php(options: $options), 'arrow' => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
    $offsetIndex = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written($writer, Schema::with($id), $rows))
        ->indexes()
        ->offsetIndex(0, $id);

    echo $engine, ': ', implode(',', array_map(static fn(PageLocation $page): int => $page->firstRowIndex, $offsetIndex->pages)), "\n";
}
foreach ([0, -1] as $maximum) {
    $options = (new Options())->set(Option::PAGE_MAXIMUM_ROWS_COUNT, $maximum);

    foreach (['php' => Writer::php(options: $options), 'arrow' => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
        echo $engine, ' ', $maximum, ': ', arrow_outcome(static fn() => MemoryParquetFile::written($writer, Schema::with($id), [['id' => 1]])), "\n";
    }
}
?>
--EXPECT--
php: 0,3,6,9
arrow: 0,3,6,9
php 0: Flow\Parquet\Exception\InvalidArgumentException: PAGE_MAXIMUM_ROWS_COUNT must be greater than 0, 0 given
arrow 0: Flow\Parquet\Exception\InvalidArgumentException: PAGE_MAXIMUM_ROWS_COUNT must be greater than 0, 0 given
php -1: Flow\Parquet\Exception\InvalidArgumentException: PAGE_MAXIMUM_ROWS_COUNT must be greater than 0, -1 given
arrow -1: Flow\Parquet\Exception\InvalidArgumentException: PAGE_MAXIMUM_ROWS_COUNT must be greater than 0, -1 given
