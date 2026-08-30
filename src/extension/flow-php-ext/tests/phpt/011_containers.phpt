--TEST--
list, map and structure columns round-trip identically to the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

$rows = rows(schema(int_schema('id'), list_schema('ints', type_list(type_integer())), list_schema('floats', type_list(type_float())), list_schema('strings', type_list(type_string())), list_schema('empty', type_list(type_integer())), list_schema('nullable_ints', type_list(type_optional(type_integer()))), map_schema('by_int', type_map(type_integer(), type_string())), map_schema('by_string', type_map(type_string(), type_float())), map_schema('empty_map', type_map(type_string(), type_integer())), structure_schema('address', type_structure([
            'street' => type_string(),
            'geo' => type_structure(['lat' => type_float(), 'lon' => type_float()]),
            'tags' => type_list(type_string()),
        ])), structure_schema('optional_absent', type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)])), structure_schema('optional_typed_null', type_structure(['a' => type_integer(), 'b' => type_optional(type_string())])), structure_schema('interleaved', type_structure(['z' => type_integer(), 'a' => structure_element('a', type_string(), optional: true), 'b' => type_string()]))), row(['id' => 1, 'ints' => [1, -2, PHP_INT_MAX], 'floats' => [1.5, -0.25], 'strings' => ['a', '', "b\x00c"], 'empty' => [], 'nullable_ints' => [1, null, 3], 'by_int' => [10 => 'a', -5 => 'b'], 'by_string' => ['cpu' => 1.5, 'mem' => 2.5], 'empty_map' => [], 'address' => [
            'street' => 'Main 7',
            'geo' => ['lat' => 1.5, 'lon' => -2.5],
            'tags' => ['x', 'y'],
        ], 'optional_absent' => ['a' => 1], 'optional_typed_null' => ['a' => 1, 'b' => null], 'interleaved' => ['z' => 1, 'b' => 'x']]));

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('ints'));
var_dump($actual[0]->get('nullable_ints'));
var_dump($actual[0]->get('by_int'));
var_dump($actual[0]->get('address')['geo']);
var_dump($actual[0]->get('optional_absent'));
var_dump($actual[0]->get('optional_typed_null'));
var_dump($actual[0]->get('interleaved'));
?>
--EXPECT--
identical
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(-2)
  [2]=>
  int(9223372036854775807)
}
array(3) {
  [0]=>
  int(1)
  [1]=>
  NULL
  [2]=>
  int(3)
}
array(2) {
  [10]=>
  string(1) "a"
  [-5]=>
  string(1) "b"
}
array(2) {
  ["lat"]=>
  float(1.5)
  ["lon"]=>
  float(-2.5)
}
array(1) {
  ["a"]=>
  int(1)
}
array(2) {
  ["a"]=>
  int(1)
  ["b"]=>
  NULL
}
array(2) {
  ["z"]=>
  int(1)
  ["b"]=>
  string(1) "x"
}
