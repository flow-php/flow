--TEST--
heterogeneous Rows conform to one union schema (columns follow union order)
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';


use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry};

$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'a')),
    row(int_entry('id', 2), str_entry('name', 'b')),
    row(int_entry('id', 3), float_entry('price', 1.5)),
    row(str_entry('name', 'c'), int_entry('id', 4)),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

var_dump(count($actual));
assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[2]->get('price')->value());
// row written as (name, id) comes back in the single union schema's column order (id, name)
var_dump(implode(',', $actual[3]->entries()->names()));
?>
--EXPECT--
int(4)
identical
float(1.5)
string(7) "id,name"
