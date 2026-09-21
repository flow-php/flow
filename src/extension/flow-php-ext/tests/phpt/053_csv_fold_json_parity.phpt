--TEST--
native CSV narrowing matches StringTypeNarrower::narrow() on the JSON parity cases
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Schema\Inference\InferredTypes;

assert_narrow_parity('json cases', InferredTypes::default()->toArray(), array_values(json_parity_cases()));
?>
--EXPECT--
json cases: identical
