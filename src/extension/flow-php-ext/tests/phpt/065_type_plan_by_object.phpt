--TEST--
a column's plan follows its Type object across the plan cache's eviction and a freed Type's reused handle
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use function Flow\ETL\DSL\{int_schema, str_schema};

$backend = new DefaultBackend();
$wrong = [];

for ($i = 0; $i < 3000; $i++) {
    $definition = $i % 2 === 0 ? int_schema('c') : str_schema('c');
    $value = $i % 2 === 0 ? $i : (string) $i;
    $builder = $backend->builder($definition);
    $builder->append($value);
    $column = $builder->finish();

    if ($column->type()->normalize() !== $definition->type()->normalize() || $column->value(0) !== $value) {
        $wrong[] = $i;
    }

    unset($definition, $builder, $column);
}

echo $wrong === [] ? 'every column kept its type' : 'wrong at ' . implode(', ', array_slice($wrong, 0, 10)), "\n";
?>
--EXPECT--
every column kept its type
