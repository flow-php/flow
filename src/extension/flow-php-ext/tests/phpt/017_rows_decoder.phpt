--TEST--
RowValues pipeline decodes streamed frame bodies identically to the pure-PHP pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, datetime_entry};

$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'a')),
    row(int_entry('id', 2), str_entry('name', null)),
    row(int_entry('id', 3), float_entry('price', 1.5)),
    row(int_entry('id', 4), datetime_entry('at', new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw')))),
);

$frames = php_frames($rows);
$decoded = ext_decode_frames($frames);

var_dump(count($decoded));

assert_rows_identical(php_decode_frames($frames), $decoded);

$schemaBody = null;
$rowBody = null;

foreach ($frames as $frame) {
    if ($frame['type'] === SCHEMA_ENTRY) {
        if ($schemaBody === null) {
            $schemaBody = $frame['body'];
        }
    } elseif ($rowBody === null && $schemaBody !== null) {
        $rowBody = $frame['body'];
    }
}

try {
    (new RustFloeEncoderNative())->decode([$rowBody . "\xEF"], $schemaBody);
    echo "FAIL: no exception\n";
} catch (Flow\Floe\Exception\ExtensionException $e) {
    echo $e->getMessage(), "\n";
}
?>
--EXPECT--
int(4)
identical
flow_php row frame length does not match its content
