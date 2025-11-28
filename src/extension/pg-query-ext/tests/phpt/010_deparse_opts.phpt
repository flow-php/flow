--TEST--
pg_query_deparse_opts() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
<?php if (!function_exists("pg_query_deparse_opts")) die("skip pg_query_deparse_opts function not available"); ?>
--FILE--
<?php
// Parse a complex query to get protobuf
$protobuf = pg_query_parse_protobuf("SELECT u.id, u.name, u.email, o.order_id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status = 'completed' ORDER BY o.total DESC LIMIT 10");

// Test basic deparse without options (should work like pg_query_deparse)
$basic = pg_query_deparse_opts($protobuf);
var_dump(is_string($basic));
var_dump(strlen($basic) > 0);

// Test with pretty_print enabled
$pretty = pg_query_deparse_opts($protobuf, true);
var_dump(is_string($pretty));
var_dump(strlen($pretty) > 0);

// Verify pretty printing adds newlines or indentation
var_dump(strpos($pretty, "\n") !== false || strlen($pretty) > strlen($basic));

// Test with custom indent_size
$indented = pg_query_deparse_opts($protobuf, true, 2);
var_dump(is_string($indented));

// Test with max_line_length
$wrapped = pg_query_deparse_opts($protobuf, true, 4, 40);
var_dump(is_string($wrapped));

// Test with trailing_newline
$trailing = pg_query_deparse_opts($protobuf, true, 4, 80, true);
var_dump(is_string($trailing));
var_dump(substr($trailing, -1) === "\n");

// Test with commas_start_of_line
$commasStart = pg_query_deparse_opts($protobuf, true, 4, 80, false, true);
var_dump(is_string($commasStart));

// Test simple query
$simpleProtobuf = pg_query_parse_protobuf("SELECT 1");
$simple = pg_query_deparse_opts($simpleProtobuf, true);
var_dump(is_string($simple));
var_dump(strlen($simple) > 0);

// Test INSERT query
$insertProtobuf = pg_query_parse_protobuf("INSERT INTO users (name, email) VALUES ('john', 'john@example.com')");
$insertPretty = pg_query_deparse_opts($insertProtobuf, true);
var_dump(is_string($insertPretty));

// Test CREATE TABLE query
$createProtobuf = pg_query_parse_protobuf("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))");
$createPretty = pg_query_deparse_opts($createProtobuf, true);
var_dump(is_string($createPretty));

echo "All tests passed\n";
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
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
All tests passed
