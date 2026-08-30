--TEST--
null values and null-typed columns hydrate the matching definitions
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

$schema = schema(int_schema('id'), str_schema('nullable', nullable: true), null_schema('null_typed'), str_schema('present'));
$rows = rows($schema, row(['id' => 1, 'nullable' => null, 'null_typed' => null, 'present' => 'value']));

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('nullable'));
var_dump($schema->get('nullable')->isNullable());
var_dump($schema->get('nullable')->type()->toString());
var_dump($actual[0]->get('null_typed'));
var_dump($schema->get('null_typed')->isNullable());
var_dump($schema->get('null_typed')->type()->toString());
var_dump($schema->get('present')->isNullable());
var_dump($schema->get('present')->type()->toString());
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
