--TEST--
dynamic (mixed) values with nested arrays, datetime, uuid and json round-trip
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\{row, rows, int_entry, list_entry};
use function Flow\Types\DSL\{type_list, type_mixed};

$rows = rows(
    row(
        int_entry('id', 1),
        list_entry('mixed', [
            null,
            42,
            -1.5,
            true,
            false,
            "text \x00 binary",
            ['a' => 1, 0 => 'zero', 'nested' => ['x' => [1, 2]]],
            new DateTimeImmutable('2025-01-01 10:00:00.5', new DateTimeZone('UTC')),
            new Uuid('01234567-89ab-4def-8123-456789abcdef'),
            new Json('{"k":"v"}'),
        ], type_list(type_mixed())),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

$mixed = $actual[0]->get('mixed')->value();
var_dump($mixed[6]);
var_dump($mixed[7]->format('Y-m-d H:i:s.u e'));
var_dump((string) $mixed[8]);
var_dump((string) $mixed[9]);
?>
--EXPECT--
identical
array(3) {
  ["a"]=>
  int(1)
  [0]=>
  string(4) "zero"
  ["nested"]=>
  array(1) {
    ["x"]=>
    array(2) {
      [0]=>
      int(1)
      [1]=>
      int(2)
    }
  }
}
string(30) "2025-01-01 10:00:00.500000 UTC"
string(36) "01234567-89ab-4def-8123-456789abcdef"
string(9) "{"k":"v"}"
