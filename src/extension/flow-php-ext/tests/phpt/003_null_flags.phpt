--TEST--
null values and null-typed columns hydrate the matching definitions
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, null_entry};

$rows = rows(
    row(
        int_entry('id', 1),
        str_entry('nullable', null),
        null_entry('null_typed'),
        str_entry('present', 'value'),
    ),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('nullable')->value());
var_dump($actual[0]->get('nullable')->definition()->isNullable());
var_dump($actual[0]->get('nullable')->type()->toString());
var_dump($actual[0]->get('null_typed')->value());
var_dump($actual[0]->get('null_typed')->definition()->isNullable());
var_dump($actual[0]->get('null_typed')->type()->toString());
var_dump($actual[0]->get('present')->definition()->isNullable());
var_dump($actual[0]->get('present')->type()->toString());
?>
--EXPECT--
identical
NULL
bool(true)
string(6) "string"
NULL
bool(true)
string(4) "null"
bool(false)
string(6) "string"
