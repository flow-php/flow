--TEST--
list, map and structure columns round-trip identically to the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';


use function Flow\ETL\DSL\{row, rows, int_entry, list_entry, map_entry, structure_entry};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_float, type_string, type_optional};

$rows = rows(
    row(
        int_entry('id', 1),
        list_entry('ints', [1, -2, PHP_INT_MAX], type_list(type_integer())),
        list_entry('floats', [1.5, -0.25], type_list(type_float())),
        list_entry('strings', ['a', '', "b\x00c"], type_list(type_string())),
        list_entry('empty', [], type_list(type_integer())),
        list_entry('nullable_ints', [1, null, 3], type_list(type_optional(type_integer()))),
        map_entry('by_int', [10 => 'a', -5 => 'b'], type_map(type_integer(), type_string())),
        map_entry('by_string', ['cpu' => 1.5, 'mem' => 2.5], type_map(type_string(), type_float())),
        map_entry('empty_map', [], type_map(type_string(), type_integer())),
        structure_entry('address', [
            'street' => 'Main 7',
            'geo' => ['lat' => 1.5, 'lon' => -2.5],
            'tags' => ['x', 'y'],
        ], type_structure([
            'street' => type_string(),
            'geo' => type_structure(['lat' => type_float(), 'lon' => type_float()]),
            'tags' => type_list(type_string()),
        ])),
        structure_entry('optional_absent', ['a' => 1], type_structure(['a' => type_integer()], ['b' => type_string()])),
        structure_entry('optional_typed_null', ['a' => 1, 'b' => null], type_structure(['a' => type_integer(), 'b' => type_optional(type_string())])),
    ),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('ints')->value());
var_dump($actual[0]->get('nullable_ints')->value());
var_dump($actual[0]->get('by_int')->value());
var_dump($actual[0]->get('address')->value()['geo']);
var_dump($actual[0]->get('optional_absent')->value());
var_dump($actual[0]->get('optional_typed_null')->value());
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
