--TEST--
RustColumnBuilder::appendPhysicals() appends physicals as they are: nulls with or without a count, nothing for [], a null under NOT NULL and a physical of another kind refused
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\RustBackend;
use Flow\ETL\Exception\SchemaMismatchException;

use function Flow\ETL\DSL\int_schema;

foreach ([2, null] as $nullCount) {
    $builder = (new RustBackend())->builder(int_schema('id', nullable: true));
    $builder->appendPhysicals([1, null, 3, null], $nullCount);
    $column = $builder->finish();
    echo json_encode($column->values()), ' ', $column->nullCount(), "\n";
}

$builder = (new RustBackend())->builder(int_schema('id'));
$builder->appendPhysicals([]);
echo $builder->count(), "\n";

try {
    $builder->appendPhysicals([1, null]);
} catch (SchemaMismatchException $e) {
    echo get_class($e), ' row ', $e->rowIndex, ' kept ', $builder->count(), "\n";
}

try {
    $builder->appendPhysicals(['not an int']);
} catch (Throwable $e) {
    echo get_class($e), ' kept ', $builder->count(), "\n";
}
?>
--EXPECT--
[1,null,3,null] 2
[1,null,3,null] 2
0
Flow\ETL\Exception\SchemaMismatchException row 1 kept 0
Flow\ETL\Exception\InvalidArgumentException kept 0
