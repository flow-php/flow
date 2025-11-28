--TEST--
pg_query_summary() functionality
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
// Test basic summary
$summary = pg_query_summary("SELECT * FROM users WHERE id = 1");
var_dump(is_string($summary));
var_dump(strlen($summary) > 0);

// Test with options (default parse mode)
$summary2 = pg_query_summary("SELECT 1", PG_QUERY_PARSE_DEFAULT);
var_dump(is_string($summary2));

// Test with truncation
$summary3 = pg_query_summary("SELECT * FROM users WHERE name = 'very long string here'", 0, 10);
var_dump(is_string($summary3));

// Verify parse mode constants are defined
var_dump(defined('PG_QUERY_PARSE_DEFAULT'));
var_dump(defined('PG_QUERY_PARSE_TYPE_NAME'));
var_dump(defined('PG_QUERY_PARSE_PLPGSQL_EXPR'));
var_dump(defined('PG_QUERY_PARSE_PLPGSQL_ASSIGN1'));
var_dump(defined('PG_QUERY_PARSE_PLPGSQL_ASSIGN2'));
var_dump(defined('PG_QUERY_PARSE_PLPGSQL_ASSIGN3'));

// Verify GUC option constants are defined
var_dump(defined('PG_QUERY_PARSE_OPTS_DISABLE_BACKSLASH_QUOTE'));
var_dump(defined('PG_QUERY_PARSE_OPTS_DISABLE_STANDARD_CONFORMING_STRINGS'));
var_dump(defined('PG_QUERY_PARSE_OPTS_DISABLE_ESCAPE_STRING_WARNING'));

// Verify constant values
var_dump(PG_QUERY_PARSE_DEFAULT === 0);
var_dump(PG_QUERY_PARSE_TYPE_NAME === 1);
var_dump(PG_QUERY_PARSE_PLPGSQL_EXPR === 2);
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
bool(true)
bool(true)
