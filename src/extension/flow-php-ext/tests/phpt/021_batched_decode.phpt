--TEST--
RustFloeEncoderNative + RowHydrator decode a batch identically to the pure-PHP pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

$build = static fn(int $i): Flow\ETL\Row => row(['id' => $i, 'name' => $i % 7 === 0 ? null : 'user_' . $i, 'price' => $i / 4.0]);

$rows = rows(
    schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price')),
    ...array_map($build, range(1, 500)),
);
$frames = php_frames($rows);

$schemaBody = null;
$rowBodies = [];

foreach ($frames as $frame) {
    if ($frame['type'] === SCHEMA_ENTRY) {
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
