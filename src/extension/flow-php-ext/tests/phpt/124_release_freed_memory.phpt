--TEST--
RustBackend::releaseFreedMemory() exists, returns null and leaves allocatedBytes() as it was
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\RustBackend;

$backend = new RustBackend();
$before = $backend->allocatedBytes();

var_dump(method_exists($backend, 'releaseFreedMemory'));
var_dump($backend->releaseFreedMemory());
var_dump($backend->allocatedBytes() === $before);
?>
--EXPECT--
bool(true)
NULL
bool(true)
