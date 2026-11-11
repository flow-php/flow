--TEST--
allocatedPeakBytes() keeps the highest allocatedBytes() since resetAllocatedPeak(), and the reset starts it at the current total
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Column\Frame\NoopCodec;

$backend = new RustBackend();
$schema = all_types_schema();
$frame = (new PhpBackend())->encodeFrame(php_rows($schema, all_types_values()), new NoopCodec());

$backend->resetAllocatedPeak();
$before = $backend->allocatedBytes();
var_dump($backend->allocatedPeakBytes() === $before);

$rows = [];

for ($i = 0; $i < 200; $i++) {
    $rows[] = $backend->decodeFrame($frame, $schema, new NoopCodec());
}

$held = $backend->allocatedBytes();
$rows = [];
gc_collect_cycles();

var_dump($held > $before);
var_dump($backend->allocatedPeakBytes() >= $held);
var_dump($backend->allocatedBytes() < $held);

$backend->resetAllocatedPeak();
var_dump($backend->allocatedPeakBytes() === $backend->allocatedBytes());
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
