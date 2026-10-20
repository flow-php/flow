--TEST--
arrow reads a plan through ReadPlanConverter::toExtension(), called by name: file-absolute (row count, skip) selectors, the row filter tree and the limit counted after it
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Engine\Arrow\ReadPlanConverter;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\Reader\{ReadPlan, RowSelection, RowSelector};
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

use function Flow\Parquet\DSL\parquet_column;

$selection = new RowSelection(new RowSelector(1, true), new RowSelector(2, false), new RowSelector(2, true), new RowSelector(1, false));
$plan = new ReadPlan($selection, $selection, new InMemoryIndexes());

var_dump(is_callable([ReadPlanConverter::class, 'toExtension']));
echo json_encode(ReadPlanConverter::toExtension($plan)), "\n";

$file = new RustParquetFileReader(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
$ids = [];

foreach (new RustColumnsReader($file, ['id'], 10, $plan) as $chunk) {
    array_push($ids, ...$chunk['id']);
}

echo json_encode($ids), "\n";

echo arrow_outcome(static fn() => new RustColumnsReader($file, ['id'], 10, ReadPlan::window(5, new InMemoryIndexes()))), "\n";

$filter = BoundFilter::bind(
    new All(parquet_column('id')->in([2, 3, 6]), parquet_column('id')->isNull()->negate(), parquet_column('missing')->isNull()),
    $file->footer()->schema(),
    new Options(),
);
$filtered = new ReadPlan(RowSelection::all(6), RowSelection::none(6), new InMemoryIndexes(), $filter, 2);

echo json_encode(ReadPlanConverter::toExtension($filtered)), "\n";

$ids = [];

foreach (new RustColumnsReader($file, ['id'], 10, $filtered) as $chunk) {
    array_push($ids, ...$chunk['id']);
}

echo json_encode($ids), "\n";
?>
--EXPECT--
bool(true)
{"selectors":[[1,true],[2,false],[2,true],[1,false]],"limit":null}
[2,3,6]
Flow\Parquet\Exception\InvalidArgumentException: A read plan over 5 rows cannot read a file of 6 rows
{"selectors":[[6,false]],"filter":{"and":[{"leaf":"intervals","path":["id"],"intervals":[[2,true,2,true],[3,true,3,true],[6,true,6,true]]},{"not":{"leaf":"is_null","path":["id"]}},{"leaf":"absent","is_null":true}]},"limit":2}
[2,3]
