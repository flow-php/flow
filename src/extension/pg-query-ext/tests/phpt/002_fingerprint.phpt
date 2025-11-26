--TEST--
pg_query_fingerprint() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
$fp1 = pg_query_fingerprint('SELECT * FROM users WHERE id = 1');
$fp2 = pg_query_fingerprint('SELECT * FROM users WHERE id = 2');

var_dump(is_string($fp1));
var_dump(strlen($fp1) > 0);
var_dump($fp1 === $fp2);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
