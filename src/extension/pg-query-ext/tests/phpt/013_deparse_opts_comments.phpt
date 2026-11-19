--TEST--
pg_query_deparse_opts() keeps the comments of the comments_from source
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
<?php if (!function_exists("pg_query_deparse_opts")) die("skip pg_query_deparse_opts function not available"); ?>
--FILE--
<?php
$sample = "SELECT a, -- the a column\n  b\n/* block comment\n   two lines */\nFROM t\nWHERE x = $1 -- trailing\n  AND y IN (1, 2);";
$tree = pg_query_parse_protobuf($sample);

var_dump(pg_query_deparse_opts($tree, true, 2, 80, false, false, $sample));
var_dump(pg_query_deparse_opts($tree, false, 2, 80, false, false, $sample));
var_dump(pg_query_deparse_opts($tree, true, 2, 80, false, false, null) === pg_query_deparse_opts($tree, true, 2));

$multi = "SELECT 1; -- a\nSELECT 2; /* b */";
var_dump(pg_query_deparse_opts(pg_query_parse_protobuf($multi), true, 2, 80, false, false, $multi));
var_dump(pg_query_deparse_opts(pg_query_parse_protobuf($multi), false, 2, 80, false, false, $multi));

try {
    pg_query_deparse_opts($tree, true, 2, 80, false, false, 'SELECT 1 /* open');
} catch (RuntimeException $e) {
    var_dump($e->getMessage());
}

foreach (['SELECT 1, 2, 3 FROM t WHERE a = 1', 'SELECT 1 LIMIT 5'] as $grown) {
    $grownTree = pg_query_parse_protobuf($grown);

    foreach ([true, false] as $pretty) {
        $out = pg_query_deparse_opts($grownTree, $pretty, 2, 80, false, false, 'SELECT 1 -- x');
        var_dump($out);
        var_dump(pg_query_deparse(pg_query_parse_protobuf($out)) === pg_query_deparse($grownTree));
    }
}
?>
--EXPECT--
string(122) "SELECT
  a,
  -- the a column
  b
FROM t
WHERE
  /* block comment
   two lines */
  x = $1
  AND -- trailing
  y IN (1, 2)"
string(109) "SELECT a, -- the a column
b FROM t WHERE 
/* block comment
   two lines */
x = $1 AND -- trailing
y IN (1, 2)"
bool(true)
string(31) "SELECT 1; -- a
SELECT 2
/* b */"
string(32) "SELECT 1; -- a
 SELECT 2 /* b */"
string(44) "unterminated /* comment at or near "/* open""
string(44) "SELECT
  1,
  -- x
  2, 3
FROM t
WHERE a = 1"
bool(true)
string(38) "SELECT 1, -- x
2, 3 FROM t WHERE a = 1"
bool(true)
string(25) "SELECT 1
LIMIT
  -- x
  5"
bool(true)
string(21) "SELECT 1 LIMIT -- x
5"
bool(true)
