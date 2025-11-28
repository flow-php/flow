--TEST--
pg_query_deparse() basic functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
// Test round-trip: parse to protobuf, then deparse back to SQL
$sql = "SELECT id, name FROM users WHERE active = true";
$protobuf = pg_query_parse_protobuf($sql);
$deparsed = pg_query_deparse($protobuf);

// The deparsed SQL should be functionally equivalent (may have slight formatting differences)
echo "Original: $sql\n";
echo "Deparsed: $deparsed\n";

// Verify round-trip by parsing again
$protobuf2 = pg_query_parse_protobuf($deparsed);
$deparsed2 = pg_query_deparse($protobuf2);
var_dump($deparsed === $deparsed2);

// Test with more complex query
$sql2 = "SELECT u.id, u.name, COUNT(*) AS total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name ORDER BY total DESC LIMIT 10";
$protobuf3 = pg_query_parse_protobuf($sql2);
$deparsed3 = pg_query_deparse($protobuf3);
echo "Complex query deparsed successfully: " . (strlen($deparsed3) > 0 ? "yes" : "no") . "\n";
?>
--EXPECTF--
Original: SELECT id, name FROM users WHERE active = true
Deparsed: SELECT id, name FROM users WHERE active = true
bool(true)
Complex query deparsed successfully: yes
