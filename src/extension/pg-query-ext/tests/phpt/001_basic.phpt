--TEST--
pg_query_parse() basic functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
$sql = "SELECT id, name FROM users WHERE active = true";
$result = pg_query_parse($sql);
$ast = json_decode($result, true);

var_dump(isset($ast['stmts']));
var_dump(isset($ast['stmts'][0]['stmt']['SelectStmt']));
?>
--EXPECT--
bool(true)
bool(true)
