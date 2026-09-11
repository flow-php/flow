--TEST--
heterogeneous Rows conform to one union schema (columns follow union order)
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

$rows = rows(schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price', nullable: true)), row(['id' => 1, 'name' => 'a']), row(['id' => 2, 'name' => 'b']), row(['id' => 3, 'price' => 1.5]), row(['name' => 'c', 'id' => 4]));

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

var_dump(count($actual));
assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[2]->get('price'));
// row written as (name, id) comes back in the single union schema's column order
var_dump(implode(',', $actual[3]->names()));
?>
--EXPECT--
int(4)
identical
float(1.5)
string(13) "id,name,price"
