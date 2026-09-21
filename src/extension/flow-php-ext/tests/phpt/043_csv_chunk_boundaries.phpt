--TEST--
native CSV rows do not depend on where chunk boundaries fall
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$raw = "\xEF\xBB\xBFid,name,note\r\n1,\"multi\r\nline\",\"a\\\"b\"\n2,\"x\"\"y\",  \"z\"\n\n3,plain,\"open\nat eof";
$reference = csv_native_rows($raw, ',', '"', '\\', chunk: strlen($raw));

assert_csv_identical('whole input vs PHP path', csv_php_rows($raw, ',', '"', '\\'), $reference);

foreach ([1, 2, 3, 7, 4096] as $chunk) {
    assert_csv_identical("chunk {$chunk}", $reference, csv_native_rows($raw, ',', '"', '\\', chunk: $chunk));
}
?>
--EXPECT--
whole input vs PHP path: identical
chunk 1: identical
chunk 2: identical
chunk 3: identical
chunk 7: identical
chunk 4096: identical
