--TEST--
RowValues pipeline decodes streamed frame bodies identically to the pure-PHP pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

use Flow\Floe\RustFloeEncoderNative;

$rows = rows(schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price', nullable: true), datetime_schema('at', nullable: true)), row(['id' => 1, 'name' => 'a']), row(['id' => 2, 'name' => null]), row(['id' => 3, 'price' => 1.5]), row(['id' => 4, 'at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw'))]));

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
