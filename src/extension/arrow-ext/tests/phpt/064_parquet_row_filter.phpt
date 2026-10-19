--TEST--
arrow reads exactly the rows a mixed row filter keeps from the filter parity fixture, as the PHP engine does: nulls, NaN, -0.0, a struct leaf, a decimal, a timestamp; an unbound conjunct is left to the caller
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

use function Flow\Parquet\DSL\{parquet_all, parquet_any, parquet_column, parquet_not};

$path = __DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/Engine/Fixtures/Filters/row_groups_arrow.parquet';

foreach ([
    parquet_all(
        parquet_not(parquet_column('score')->lessThan(30.0)),
        parquet_any(parquet_column('maybe')->isNull(), parquet_column('address.city')->equals('c3')),
        parquet_column('label')->equals(10),
    ),
    parquet_all(
        parquet_column('score')->equals(0.0),
        parquet_not(parquet_column('amount')->greaterThan(20.25)),
        parquet_column('ts')->greaterThanOrEqual(new DateTimeImmutable('2024-01-01 00:00:00 UTC')),
    ),
    parquet_column('missing')->isNull()->and(parquet_column('id')->lessThan(3)),
] as $filter) {
    $php = Reader::php()->read($path);
    $plan = (new ReadPlanner())->plan(
        $php->reader()->footer(),
        $php->indexes(),
        BoundFilter::bind($filter, $php->schema(), new Options()),
    );
    $ids = static function (Reader $reader) use ($path, $plan): array {
        $ids = [];
        $file = $reader->read($path);

        foreach ($file->reader()->readColumns(['id'], 7, $plan) as $chunk) {
            array_push($ids, ...$chunk['id']);
        }

        return $ids;
    };
    $arrow = $ids(new Reader(engine: new RustParquetEngine()));

    echo json_encode($arrow), ' ', var_export($arrow === $ids(Reader::php()), true), "\n";
}
?>
--EXPECT--
[11,31,32,33,34,36,37,38,39] true
[0,10] true
[0,1,2] true
