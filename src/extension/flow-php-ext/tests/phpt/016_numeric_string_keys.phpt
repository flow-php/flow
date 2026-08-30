--TEST--
array-key semantics: numeric column names coerce, non-canonical keys stay strings
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

// PHP arrays can never carry canonical numeric-string keys (the engine coerces
// them to ints before serialization), so the wire only holds non-canonical
// string keys ("05", "5x", "-0", ...) and integer keys. What MUST coerce on
// decode: numeric COLUMN names, because Row stores values name-keyed.
$rows = rows(schema(int_schema('id'), str_schema('7'), map_schema('keys', type_map(type_string(), type_string()))), row(['id' => 1, '7' => 'numeric column name', 'keys' => [
            '05' => 'leading-zero',
            '5x' => 'suffix',
            '-0' => 'minus-zero',
            '9223372036854775808' => 'overflow',
        ]]));

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump(array_map(fn($k) => gettype($k) . ':' . $k, array_keys($actual[0]->get('keys'))));
var_dump($actual[0]->get('7'));
var_dump($actual[0]->names());
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
string(19) "numeric column name"
array(3) {
  [0]=>
  string(2) "id"
  [1]=>
  int(7)
  [2]=>
  string(4) "keys"
}