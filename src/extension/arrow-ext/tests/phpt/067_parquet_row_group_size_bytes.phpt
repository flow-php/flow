--TEST--
ROW_GROUP_SIZE_BYTES below 1 is refused by both engines alike
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
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;

foreach ([0, -1] as $bytes) {
    $options = (new Options())->set(Option::ROW_GROUP_SIZE_BYTES, $bytes);

    foreach (['php' => Writer::php(options: $options), 'arrow' => new Writer(options: $options, engine: new RustParquetEngine())] as $engine => $writer) {
        echo $engine, ' ', $bytes, ': ', arrow_outcome(static fn() => MemoryParquetFile::written($writer, Schema::with(FlatColumn::int64('id')), [['id' => 1]])), "\n";
    }
}
?>
--EXPECT--
php 0: Flow\Parquet\Exception\InvalidArgumentException: ROW_GROUP_SIZE_BYTES must be greater than 0, 0 given
arrow 0: Flow\Parquet\Exception\InvalidArgumentException: ROW_GROUP_SIZE_BYTES must be greater than 0, 0 given
php -1: Flow\Parquet\Exception\InvalidArgumentException: ROW_GROUP_SIZE_BYTES must be greater than 0, -1 given
arrow -1: Flow\Parquet\Exception\InvalidArgumentException: ROW_GROUP_SIZE_BYTES must be greater than 0, -1 given
