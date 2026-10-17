--TEST--
arrow reads a plan through ReadPlanConverter::toExtension(), called by name: file-absolute (row count, skip) selectors
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Engine\Arrow\ReadPlanConverter;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Reader\{ReadPlan, RowSelection, RowSelector};
use Flow\Parquet\Tests\Context\MemoryParquetFile;

$selection = new RowSelection(new RowSelector(1, true), new RowSelector(2, false), new RowSelector(2, true), new RowSelector(1, false));
$plan = new ReadPlan($selection, $selection);

var_dump(is_callable([ReadPlanConverter::class, 'toExtension']));
echo json_encode(ReadPlanConverter::toExtension($plan)), "\n";

$file = new RustParquetFileReader(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
$ids = [];

foreach (new RustColumnsReader($file, ['id'], 10, $plan) as $chunk) {
    array_push($ids, ...$chunk['id']);
}

echo json_encode($ids), "\n";

echo arrow_outcome(static fn() => new RustColumnsReader($file, ['id'], 10, ReadPlan::window(5))), "\n";
?>
--EXPECT--
bool(true)
{"selectors":[[1,true],[2,false],[2,true],[1,false]]}
[2,3,6]
Flow\Parquet\Exception\InvalidArgumentException: A read plan over 5 rows cannot read a file of 6 rows
