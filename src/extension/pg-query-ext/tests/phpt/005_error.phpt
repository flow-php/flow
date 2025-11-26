--TEST--
pg_query_parse() throws exception on invalid SQL
--SKIPIF--
<?php if (!extension_loaded("pg_query")) die("skip pg_query extension not loaded"); ?>
--FILE--
<?php
try {
    pg_query_parse('SELECT * FROM WHERE');
    echo "No exception thrown\n";
} catch (RuntimeException $e) {
    echo "Exception caught\n";
    var_dump(strlen($e->getMessage()) > 0);
}
?>
--EXPECT--
Exception caught
bool(true)
