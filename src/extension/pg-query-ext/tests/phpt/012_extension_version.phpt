--TEST--
extension version is the release tag, or the last tag with commit distance and hash
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
var_dump(preg_match('/^\d+\.\d+\.\d+(\+\d+\.g[0-9a-f]+)?$/', phpversion('pg_query')));
?>
--EXPECT--
int(1)
