--TEST--
arrow reads a page-pruned, row-filtered plan over a parquet-mr file whose columns break pages at other rows (int64 at 0, 200, 428, 628, 828; float at 0, 300, 628, 928): exactly the matching rows, the same as the PHP engine
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ReadPlanner;

use function Flow\Parquet\DSL\parquet_column;

$path = __DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures/delta_binary_acked_encoded_integers.parquet';
$columns = ['int32_column', 'int64_column', 'float_column'];

foreach ([parquet_column('int64_column')->lessThan(300), parquet_column('float_column')->lessThan(400.0)] as $filter) {
    $php = Reader::php()->read($path);
    $plan = (new ReadPlanner())->plan(
        $php->reader()->footer(),
        $php->indexes(),
        BoundFilter::bind($filter, $php->schema(), new Options()),
    );
    $read = static function (Reader $reader) use ($path, $columns, $plan): array {
        $rows = [];
        $file = $reader->read($path);

        foreach ($file->reader()->readColumns($columns, 100, $plan) as $chunk) {
            foreach (array_keys($chunk['int32_column']) as $index) {
                $rows[] = array_map(static fn(string $column): mixed => $chunk[$column][$index], $columns);
            }
        }

        return $rows;
    };
    $arrow = $read(new Reader(engine: new RustParquetEngine()));

    echo json_encode(array_map(
        static fn($selector): array => [$selector->rowCount, $selector->skip],
        $plan->selection->selectors(),
    )), ' ', count($arrow), ' ', var_export($arrow === $read(Reader::php()), true), "\n";
}

?>
--EXPECT--
[[428,false],[572,true]] 270 true
[[628,false],[372,true]] 342 true
