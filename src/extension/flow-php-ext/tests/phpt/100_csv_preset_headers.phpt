--TEST--
native CSV preset headers key every record - the first one included - exactly like CSVDecoder's, and refuse a header record
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$cases = [
    'plain' => ["1,a\n2,b\n", ['id', 'name']],
    'ragged' => ["1\n1,2,3\n,,\n", ['id', 'name']],
    'duplicated name' => ["1,2,3\n", ['id', 'name', 'id']],
    'integer-like name' => ["1,2\n", ['id', '5']],
    'quoted multi-line' => ["\"x\ny\",1\n2,3\n", ['text', 'n']],
    'empty stream' => ['', ['id']],
];

foreach ($cases as $label => [$raw, $headers]) {
    foreach ([1, 3, 4096] as $chunk) {
        $expected = csv_php_rows($raw, ',', '"', '\\', false, true, false, $headers);
        $actual = csv_native_rows($raw, ',', '"', '\\', false, true, false, $chunk, $headers);

        assert_csv_identical(sprintf('%s chunk=%d', $label, $chunk), $expected, $actual);
    }
}

try {
    new Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource(
        new Flow\Filesystem\Stream\StringSourceStream(Flow\Filesystem\DSL\path('memory://phpt.csv'), "1,a\n"),
        ',',
        '"',
        '\\',
        true,
        true,
        true,
        4096,
        ['id'],
    );
} catch (Throwable $e) {
    echo $e->getMessage(), "\n";
}

[, $rows] = csv_native_rows("1,a\n", ',', '"', '\\', false, true, false, 4096, ['id', 'name']);
var_dump($rows);
?>
--EXPECT--
plain chunk=1: identical
plain chunk=3: identical
plain chunk=4096: identical
ragged chunk=1: identical
ragged chunk=3: identical
ragged chunk=4096: identical
duplicated name chunk=1: identical
duplicated name chunk=3: identical
duplicated name chunk=4096: identical
integer-like name chunk=1: identical
integer-like name chunk=3: identical
integer-like name chunk=4096: identical
quoted multi-line chunk=1: identical
quoted multi-line chunk=3: identical
quoted multi-line chunk=4096: identical
empty stream chunk=1: identical
empty stream chunk=3: identical
empty stream chunk=4096: identical
flow_php CSV preset headers replace the header record, withHeader must be false
array(1) {
  [0]=>
  array(2) {
    ["id"]=>
    string(1) "1"
    ["name"]=>
    string(1) "a"
  }
}
