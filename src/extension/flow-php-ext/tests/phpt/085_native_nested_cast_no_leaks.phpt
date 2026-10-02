--TEST--
nested values cast natively, handed to the PHP lane half-appended, and refused leak neither PHP memory nor native allocations
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\RustBackend;

use function Flow\ETL\DSL\{list_schema, map_schema, structure_schema};
use function Flow\Types\DSL\{structure_element, type_datetime, type_integer, type_list, type_map, type_optional, type_string, type_structure};

$backend = new RustBackend();
$orders = structure_schema('order', type_structure([
    'id' => structure_element('id', type_integer()),
    'tags' => structure_element('tags', type_list(type_string())),
    'at' => structure_element('at', type_optional(type_datetime())),
    'note' => structure_element('note', type_string(), optional: true),
]));
$lists = list_schema('lists', type_list(type_list(type_integer())));
$maps = map_schema('maps', type_map(type_string(), type_list(type_string())));

$cycle = static function () use ($backend, $orders, $lists, $maps): void {
    $builder = $backend->builder($orders);
    $builder->appendMany([
        ['id' => 1, 'tags' => ['a', 'b'], 'at' => '2026-01-02T03:04:05Z', 'note' => 'n'],
        ['id' => '2', 'tags' => [], 'at' => null],
        ['id' => 3, 'tags' => '["json", "text"]', 'at' => null],
    ]);
    $builder->finish()->values();
    $backend->builder($lists)->appendMany([[[1, 2], [3]], [[4, '5']], [[6, 7], [8]]]);
    $backend->builder($maps)->appendMany([['a' => ['x'], 'b' => []], ['c' => ['y', 'z']]]);

    try {
        $backend->builder($orders)->appendMany([['id' => 1, 'tags' => ['a'], 'at' => null], ['id' => 2, 'tags' => ['b', null]]]);
        throw new LogicException('a refused value was accepted');
    } catch (Flow\ETL\Exception\SchemaMismatchException) {
    }

    try {
        $backend->builder($lists)->append([[1, 2], ['not an int']]);
        throw new LogicException('a refused value was accepted');
    } catch (Flow\Types\Exception\CastingException) {
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = $backend->allocatedBytes();

for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump($backend->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
