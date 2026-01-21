--TEST--
pg_query_is_utility_stmt() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
// DML statements should return false
var_dump(pg_query_is_utility_stmt('SELECT * FROM users'));
var_dump(pg_query_is_utility_stmt('INSERT INTO users VALUES (1)'));
var_dump(pg_query_is_utility_stmt('UPDATE users SET name = $1'));
var_dump(pg_query_is_utility_stmt('DELETE FROM users WHERE id = 1'));

// DDL/utility statements should return true
var_dump(pg_query_is_utility_stmt('CREATE TABLE users (id int)'));
var_dump(pg_query_is_utility_stmt('ALTER TABLE users ADD COLUMN name text'));
var_dump(pg_query_is_utility_stmt('DROP TABLE users'));
var_dump(pg_query_is_utility_stmt('CREATE INDEX idx ON users (id)'));
?>
--EXPECT--
bool(false)
bool(false)
bool(false)
bool(false)
bool(true)
bool(true)
bool(true)
bool(true)
