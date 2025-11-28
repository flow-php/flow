--TEST--
pg_query_parse_protobuf() basic functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
$sql = "SELECT id, name FROM users";
$protobuf = pg_query_parse_protobuf($sql);

// Should return non-empty binary data
var_dump(strlen($protobuf) > 0);

// Should be different from JSON parse result
$json = pg_query_parse($sql);
var_dump($protobuf !== $json);

// Multiple parses should return the same result
$protobuf2 = pg_query_parse_protobuf($sql);
var_dump($protobuf === $protobuf2);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
