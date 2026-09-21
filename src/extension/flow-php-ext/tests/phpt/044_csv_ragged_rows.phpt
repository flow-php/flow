--TEST--
native CSV rows pad and truncate to the header like CSVRowNormalizer, under both emptyToNull settings
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$raw = "id, name ,,5,id\n1\n1,2,3,4,5,6,7\n,,\n\n";

foreach ([true, false] as $emptyToNull) {
    foreach ([true, false] as $withHeader) {
        $expected = csv_php_rows($raw, ',', '"', '\\', $withHeader, $emptyToNull);
        $actual = csv_native_rows($raw, ',', '"', '\\', $withHeader, $emptyToNull);

        assert_csv_identical(sprintf('emptyToNull=%d withHeader=%d', $emptyToNull, $withHeader), $expected, $actual);
    }
}

[$headers, $rows] = csv_native_rows($raw, ',', '"', '\\');
var_dump($headers);
var_dump($rows[0]);
?>
--EXPECT--
emptyToNull=1 withHeader=1: identical
emptyToNull=1 withHeader=0: identical
emptyToNull=0 withHeader=1: identical
emptyToNull=0 withHeader=0: identical
array(5) {
  [0]=>
  string(2) "id"
  [1]=>
  string(4) "name"
  [2]=>
  string(3) "e02"
  [3]=>
  string(1) "5"
  [4]=>
  string(2) "id"
}
array(4) {
  ["id"]=>
  NULL
  ["name"]=>
  NULL
  ["e02"]=>
  NULL
  [5]=>
  NULL
}
