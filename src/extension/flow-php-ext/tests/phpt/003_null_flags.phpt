--TEST--
null and from-null value flags hydrate the matching definition variants
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Schema\Metadata;
use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, null_entry};

$rows = rows(
    row(
        int_entry('id', 1),
        str_entry('nullable', null),
        null_entry('from_null'),
        str_entry('present', 'value'),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('nullable')->value());
var_dump($actual[0]->get('nullable')->definition()->isNullable());
var_dump($actual[0]->get('nullable')->definition()->metadata()->has(Metadata::FROM_NULL));
var_dump($actual[0]->get('from_null')->value());
var_dump($actual[0]->get('from_null')->definition()->isNullable());
var_dump($actual[0]->get('from_null')->definition()->metadata()->get(Metadata::FROM_NULL));
var_dump($actual[0]->get('present')->definition()->isNullable());
var_dump($actual[0]->get('present')->definition()->metadata()->has(Metadata::FROM_NULL));
?>
--EXPECT--
identical
NULL
bool(true)
bool(false)
NULL
bool(true)
bool(true)
bool(false)
bool(false)
