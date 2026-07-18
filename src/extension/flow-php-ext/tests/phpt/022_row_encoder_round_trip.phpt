--TEST--
RustFloeEncoderNative frame bodies round-trip through the RawRowValues pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, int_entry, str_entry, float_entry, datetime_entry, uuid_entry, list_entry, schema_from_json};
use function Flow\Types\DSL\{type_list, type_integer};

$sourceRows = [
    row(int_entry('id', 1), str_entry('name', 'a'), float_entry('price', 1.5), datetime_entry('at', new DateTimeImmutable('2025-01-01 10:00:00.5', new DateTimeZone('UTC'))), uuid_entry('u', '01234567-89ab-4def-8123-456789abcdef'), list_entry('nums', [1, 2, 3], type_list(type_integer()))),
    row(int_entry('id', 2), str_entry('name', null), float_entry('price', -0.25), datetime_entry('at', new DateTimeImmutable('1999-12-31 23:59:59.999999', new DateTimeZone('Europe/Warsaw'))), uuid_entry('u', 'abcdef01-2345-4678-9abc-def012345678'), list_entry('nums', [], type_list(type_integer()))),
    row(int_entry('id', 3), str_entry('name', 'c'), float_entry('price', 99.0), datetime_entry('at', new DateTimeImmutable('2030-06-15 08:30:00', new DateTimeZone('America/New_York'))), uuid_entry('u', '11111111-2222-4333-8444-555566667777'), list_entry('nums', [-1, 0, PHP_INT_MAX], type_list(type_integer()))),
];

$schemaBody = json_encode($sourceRows[0]->schema()->normalize(), JSON_THROW_ON_ERROR);
$hydrator = new PhpRowHydrator();

$encoder = new RustFloeEncoderNative();
$bodies = $encoder->encode($hydrator->dehydrate(new Rows(...$sourceRows)), $schemaBody);

$decoded = $hydrator->hydrate((new RustFloeEncoderNative())->decode($bodies, $schemaBody), schema_from_json($schemaBody));

var_dump($decoded instanceof Rows);
var_dump($decoded->count());

$reEncoder = new RustFloeEncoderNative();
$reBodies = $reEncoder->encode($hydrator->dehydrate($decoded), $schemaBody);
var_dump($bodies === $reBodies);

$frames = array_merge(
    [['type' => Format::FRAME_SCHEMA, 'body' => $schemaBody]],
    array_map(fn($body) => ['type' => Format::FRAME_ROW, 'body' => $body], $bodies),
);
assert_rows_identical(php_decode_frames($frames), $decoded->all());

var_dump($decoded->all()[1]->get('name')->value());
var_dump($decoded->all()[2]->get('nums')->value());
?>
--EXPECT--
bool(true)
int(3)
bool(true)
identical
NULL
array(3) {
  [0]=>
  int(-1)
  [1]=>
  int(0)
  [2]=>
  int(9223372036854775807)
}
