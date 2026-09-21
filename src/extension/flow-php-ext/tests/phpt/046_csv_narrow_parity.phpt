--TEST--
native narrowing matches StringTypeNarrower::narrow() on its fixtures, the byte-level traps and a fuzz corpus
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Schema\Inference\InferredTypes;

$fixtures = string_type_narrower_fixture_values();
$corpus = csv_narrow_corpus();

var_dump(count($fixtures) >= 80);
var_dump(count($corpus) >= 300);
assert_narrow_parity('default candidates', InferredTypes::default()->toArray(), $corpus);
?>
--EXPECT--
bool(true)
bool(true)
default candidates: identical
