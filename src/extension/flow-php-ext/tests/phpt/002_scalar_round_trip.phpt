--TEST--
scalar columns round-trip identically to the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, bool_entry};

$rows = rows(
    row(
        int_entry('id', 0),
        str_entry('name', null),
        str_entry('email', "binary \x00 unsafe \n string"),
        float_entry('price', 12345.678),
        bool_entry('active', true),
        int_entry('score', -1000),
    ),
    row(
        int_entry('id', PHP_INT_MAX),
        str_entry('name', 'user_1'),
        str_entry('email', ''),
        float_entry('price', -0.5),
        bool_entry('active', false),
        int_entry('score', PHP_INT_MIN),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

var_dump(count($actual) === 2);
assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('id')->value());
var_dump($actual[0]->get('name')->value());
var_dump($actual[0]->get('name')->definition()->isNullable());
var_dump($actual[0]->get('email')->value() === "binary \x00 unsafe \n string");
var_dump($actual[0]->get('email')->definition()->isNullable());
var_dump($actual[1]->get('id')->value() === PHP_INT_MAX);
var_dump($actual[1]->get('score')->value() === PHP_INT_MIN);
var_dump($actual[1]->get('price')->value());
var_dump($actual[1]->get('active')->value());
?>
--EXPECT--
bool(true)
identical
int(0)
NULL
bool(true)
bool(true)
bool(false)
bool(true)
bool(true)
float(-0.5)
bool(false)
