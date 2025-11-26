--TEST--
pg_query_split() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
$sql = "SELECT 1; SELECT 2; SELECT 3";
$statements = pg_query_split($sql);

var_dump(is_array($statements));
var_dump(count($statements));
?>
--EXPECT--
bool(true)
int(3)
