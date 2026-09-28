--TEST--
decode, value reads, take, encode and a refused appendMany leak neither PHP memory nor native allocations
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\Floe\FrameDecoder;

$schema = all_types_schema();
$frame = php_rows($schema, all_types_values())->encodeFrame();
$backend = new DefaultBackend();

$cycle = static function () use ($schema, $frame, $backend): void {
    $rows = (new FrameDecoder())->decode($frame, $schema, $backend);

    foreach ($rows->columns() as $column) {
        $column->value(1);
        $column->take([2, 0])->encode();
    }

    try {
        $backend->builder($schema->get('int'))->appendMany([1, 'not an int']);
        throw new LogicException('a refused value was accepted');
    } catch (Flow\ETL\Exception\SchemaMismatchException) {
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
