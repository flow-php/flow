--TEST--
pg_query_normalize_utility() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
// Test CREATE TABLE - utility statements preserve their structure
$normalized = pg_query_normalize_utility("CREATE TABLE users (name VARCHAR(255) DEFAULT 'unknown', age INT DEFAULT 18)");
var_dump(is_string($normalized));
var_dump(str_contains($normalized, 'CREATE TABLE'));
var_dump(str_contains($normalized, 'users'));

// Test ALTER TABLE
$normalized2 = pg_query_normalize_utility("ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'");
var_dump(is_string($normalized2));
var_dump(str_contains($normalized2, 'ALTER TABLE'));

// Test DROP
$normalized3 = pg_query_normalize_utility("DROP TABLE IF EXISTS users");
var_dump(is_string($normalized3));
var_dump(str_contains($normalized3, 'DROP TABLE'));

// Verify function returns different result than pg_query_normalize for DML
$dml = "SELECT * FROM users WHERE name = 'John' AND age = 30";
$normalizedDml = pg_query_normalize($dml);
$utilityDml = pg_query_normalize_utility($dml);
var_dump(str_contains($normalizedDml, '$1'));  // pg_query_normalize replaces literals
var_dump(!str_contains($utilityDml, '$1'));    // pg_query_normalize_utility does not for DML
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
