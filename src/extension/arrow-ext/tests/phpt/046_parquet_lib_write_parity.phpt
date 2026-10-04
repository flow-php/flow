--TEST--
new Writer(engine: new RustParquetEngine()) on arrow accepts and refuses what Writer::php() does per target type, and what both accept reads back the same
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Tests\Context\WriteAcceptance;
use Flow\Parquet\Writer;

$cells = $accepted = $refused = 0;

foreach (WriteAcceptance::targets() as $target) {
    foreach (WriteAcceptance::inputs() as $input) {
        $read = static fn(Writer $writer): string => arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(
            Reader::php(),
            MemoryParquetFile::written($writer, Schema::with(WriteAcceptance::target($target)), [['c' => WriteAcceptance::input($input)]]),
        ), 1));
        $php = $read(Writer::php());
        $native = $read(new Writer(engine: new RustParquetEngine()));
        $cells++;

        match (true) {
            arrow_refused($php) && arrow_refused($native) => $refused++,
            !arrow_refused($php) && $php === $native => $accepted++,
            default => print("{$target} <- {$input}: DIFFER\n    php:    " . substr($php, 0, 200) . "\n    native: " . substr($native, 0, 200) . "\n"),
        };
    }
}

echo "{$cells} cells: {$accepted} accepted by both, {$refused} refused by both\n";
echo 'every type: ', arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(Writer::php(), EveryType::schema(), EveryType::rows())), 3)) === arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(new Writer(engine: new RustParquetEngine()), EveryType::schema(), EveryType::rows())), 3)) ? 'identical' : 'DIFFER', "\n";
?>
--EXPECT--
624 cells: 102 accepted by both, 522 refused by both
every type: identical
