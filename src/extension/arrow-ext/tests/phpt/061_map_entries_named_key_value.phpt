--TEST--
The arrow writer names a map's repeated group key_value (parquet-format LogicalTypes.md), the leaf paths the PHP writer writes
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Writer;

$schema = Schema::with(NestedColumn::map('m', MapKey::string(), MapValue::int64()));
$rows = [['m' => ['a' => 1, 'b' => 2]]];

foreach (['arrow' => new Writer(engine: new RustParquetEngine()), 'php' => Writer::php()] as $name => $writer) {
    $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written($writer, $schema, $rows));

    echo $name, ': ', implode(', ', array_map(
        static fn($column): string => $column->flatPath(),
        $file->metadata()->schema()->columnsFlat(),
    )), ' ', json_encode(ParquetRows::read($file, 10)), "\n";
}
--EXPECT--
arrow: m.key_value.key, m.key_value.value [{"m":{"a":1,"b":2}}]
php: m.key_value.key, m.key_value.value [{"m":{"a":1,"b":2}}]
