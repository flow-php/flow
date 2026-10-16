--TEST--
UINT_64 reads as its decimal string and writes from an int >= 0 or a digit string; a negative int is refused naming its row
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Writer;

$fixture = __DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/Engine/Fixtures/uint64_duckdb.parquet';
$arrow = new Reader(engine: new RustParquetEngine());
$schema = Schema::with(new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)));
$writer = static fn(): Writer => new Writer(engine: new RustParquetEngine());

echo json_encode(ParquetRows::read($arrow->read($fixture), 10)), "\n";
echo json_encode(ParquetRows::read(MemoryParquetFile::read($arrow, MemoryParquetFile::written(
    $writer(),
    $schema,
    [['u' => '18446744073709551615'], ['u' => 7], ['u' => null], ['u' => '9223372036854775808']],
)), 10)), "\n";
echo arrow_outcome(static fn() => MemoryParquetFile::written($writer(), $schema, [['u' => 1], ['u' => -1]])), "\n";
echo arrow_outcome(static fn() => MemoryParquetFile::written($writer(), $schema, [['u' => '18446744073709551616']])), "\n";
--EXPECT--
[{"col0":"0"},{"col0":"9223372036854775808"},{"col0":"18446744073709551615"}]
[{"u":"18446744073709551615"},{"u":"7"},{"u":null},{"u":"9223372036854775808"}]
Flow\Parquet\Exception\ValidationException: Column "u" row 1: expected int >= 0 or digit string, got int
Flow\Parquet\Exception\ValidationException: Column "u" row 0: expected int >= 0 or digit string, got string
