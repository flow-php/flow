--TEST--
RowsDecoder decodes streamed frame bodies identically to the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, datetime_entry};

$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'a')),
    row(int_entry('id', 2), str_entry('name', null)),
    row(int_entry('id', 3), float_entry('price', 1.5)),
    row(int_entry('id', 4), datetime_entry('at', new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw')))),
);

$frames = php_frames($rows);
$expected = php_decode_frames($frames);
$decoded = decoder_decode_frames(new RowsDecoder(), $frames);

var_dump(count($decoded));

$identical = true;
foreach ($expected as $i => $expectedRow) {
    if (serialize($expectedRow) !== serialize($decoded[$i])) {
        $identical = false;
        echo "FAIL: row {$i} differs\n";
    }
}
var_dump($identical);

$fresh = new RowsDecoder();
try {
    $fresh->row("\x01");
    echo "FAIL: no exception\n";
} catch (Flow\Floe\Exception\ExtensionException $e) {
    echo $e->getMessage(), "\n";
}
?>
--EXPECT--
int(4)
bool(true)
flow_php found a row frame before any schema frame
