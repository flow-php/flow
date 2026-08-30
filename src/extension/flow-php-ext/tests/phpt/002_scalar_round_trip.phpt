--TEST--
scalar columns round-trip identically to the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

$schema = schema(int_schema('id'), str_schema('name', nullable: true), str_schema('email'), float_schema('price'), bool_schema('active'), int_schema('score'));
$rows = rows($schema, row(['id' => 0, 'name' => null, 'email' => "binary \x00 unsafe \n string", 'price' => 12345.678, 'active' => true, 'score' => -1000]), row(['id' => PHP_INT_MAX, 'name' => 'user_1', 'email' => '', 'price' => -0.5, 'active' => false, 'score' => PHP_INT_MIN]));

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

var_dump(count($actual) === 2);
assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('id'));
var_dump($actual[0]->get('name'));
var_dump($schema->get('name')->isNullable());
var_dump($actual[0]->get('email') === "binary \x00 unsafe \n string");
var_dump($schema->get('email')->isNullable());
var_dump($actual[1]->get('id') === PHP_INT_MAX);
var_dump($actual[1]->get('score') === PHP_INT_MIN);
var_dump($actual[1]->get('price'));
var_dump($actual[1]->get('active'));
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
