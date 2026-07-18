--TEST--
RustFloeEncoderNative + RowHydrator decode a batch identically to the pure-PHP pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, schema_from_json};

$build = static fn(int $i): Flow\ETL\Row => row(
    int_entry('id', $i),
    str_entry('name', $i % 7 === 0 ? null : 'user_' . $i),
    float_entry('price', $i / 4),
);

$rows = rows(...array_map($build, range(1, 500)));
$frames = php_frames($rows);

$schemaBody = null;
$rowBodies = [];

foreach ($frames as $frame) {
    if ($frame['type'] === Format::FRAME_SCHEMA) {
        $schemaBody = $frame['body'];
    } else {
        $rowBodies[] = $frame['body'];
    }
}

var_dump(count($rowBodies));

$decoder = new RustFloeEncoderNative();
$hydrator = new PhpRowHydrator();
$schema = schema_from_json($schemaBody);

$decodedValues = $decoder->decode($rowBodies, $schemaBody);
var_dump($decodedValues[0] instanceof RawRowValues);

$batched = $hydrator->hydrate($decodedValues, $schema);
var_dump($batched instanceof Rows);
var_dump($batched->count());

$php = php_decode_frames($frames);
$batchedRows = $batched->all();

$identical = true;

foreach ($php as $i => $expected) {
    if (serialize($expected) !== serialize($batchedRows[$i])) {
        $identical = false;
        echo "FAIL: batched row {$i} differs\n";
    }
}

var_dump($identical);

$empty = $hydrator->hydrate($decoder->decode([], $schemaBody), $schema);
var_dump($empty instanceof Rows);
var_dump($empty->count());
?>
--EXPECT--
int(500)
bool(true)
bool(true)
int(500)
bool(true)
bool(true)
int(0)
