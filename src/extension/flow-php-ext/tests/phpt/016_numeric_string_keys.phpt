--TEST--
array-key semantics: numeric column names coerce, non-canonical keys stay strings
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, map_entry, list_entry};
use function Flow\Types\DSL\{type_map, type_list, type_string, type_mixed};

// PHP arrays can never carry canonical numeric-string keys (the engine coerces
// them to ints before serialization), so the wire only holds non-canonical
// string keys ("05", "5x", "-0", ...) and integer keys. What MUST coerce on
// decode: numeric ENTRY names, because Entries stores entries name-keyed.
$rows = rows(
    row(
        int_entry('id', 1),
        str_entry('7', 'numeric column name'),
        map_entry('keys', [
            '05' => 'leading-zero',
            '5x' => 'suffix',
            '-0' => 'minus-zero',
            '9223372036854775808' => 'overflow',
        ], type_map(type_string(), type_string())),
        list_entry('dynamic', [['05' => 'a', 'x' => 'b']], type_list(type_mixed())),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump(array_map(fn($k) => gettype($k) . ':' . $k, array_keys($actual[0]->get('keys')->value())));
var_dump(array_map(fn($k) => gettype($k) . ':' . $k, array_keys($actual[0]->get('dynamic')->value()[0])));
var_dump($actual[0]->get('7')->value());
var_dump($actual[0]->entries()->names());
?>
--EXPECT--
identical
array(4) {
  [0]=>
  string(9) "string:05"
  [1]=>
  string(9) "string:5x"
  [2]=>
  string(9) "string:-0"
  [3]=>
  string(26) "string:9223372036854775808"
}
array(2) {
  [0]=>
  string(9) "string:05"
  [1]=>
  string(8) "string:x"
}
string(19) "numeric column name"
array(4) {
  [0]=>
  string(2) "id"
  [1]=>
  int(7)
  [2]=>
  string(4) "keys"
  [3]=>
  string(7) "dynamic"
}