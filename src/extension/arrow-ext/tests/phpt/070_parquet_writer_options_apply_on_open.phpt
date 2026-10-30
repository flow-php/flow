--TEST--
The options given to Writer apply on open(), as on write(): ARROW_WRITE_BATCH_SIZE closes as many row groups through both
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;

use function Flow\Filesystem\DSL\path;

$options = (new Options())->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(Option::ARROW_WRITE_BATCH_SIZE, 1);
$schema = Schema::with(FlatColumn::int64('id'));
$rows = array_map(static fn(int $id): array => ['id' => $id], range(1, 4));

$writer = new Writer(options: $options, engine: new RustParquetEngine());
$writer->openForStream($opened = new StringDestinationStream(path('memory://opened.parquet')), $schema);
$writer->writeBatch($rows);
$writer->close();
$written = MemoryParquetFile::written(new Writer(options: $options, engine: new RustParquetEngine()), $schema, $rows);

foreach (['open' => $opened->content(), 'write' => $written] as $door => $bytes) {
    echo $door, ': ', count(MemoryParquetFile::read(Reader::php(), $bytes)->metadata()->rowGroups()->all()), "\n";
}
?>
--EXPECT--
open: 4
write: 4
