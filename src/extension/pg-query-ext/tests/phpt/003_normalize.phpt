--TEST--
pg_query_normalize() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
$normalized = pg_query_normalize("SELECT * FROM users WHERE name = 'John' AND age = 30");

var_dump(is_string($normalized));
var_dump(str_contains($normalized, '$1'));
var_dump(str_contains($normalized, '$2'));
var_dump(!str_contains($normalized, "'John'"));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
