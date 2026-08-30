--TEST--
empty Rows produce no frame bodies and decode to an empty row list
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

$frames = php_frames(rows(schema()));
$actual = ext_decode_frames($frames);

var_dump($frames === []);
var_dump(count($actual));
assert_rows_identical(php_decode_frames($frames), $actual);
?>
--EXPECT--
bool(true)
int(0)
identical
