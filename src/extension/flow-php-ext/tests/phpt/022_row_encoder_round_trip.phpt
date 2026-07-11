--TEST--
RowsEncoder frame bodies round-trip through RowsDecoder::rows
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\FloeWriter;
use Flow\Floe\RowsDecoder;
use Flow\Floe\RowsEncoder;

use function Flow\ETL\DSL\{row, int_entry, str_entry, float_entry, datetime_entry, uuid_entry, list_entry};
use function Flow\Types\DSL\{type_list, type_integer};

$sourceRows = [
    row(int_entry('id', 1), str_entry('name', 'a'), float_entry('price', 1.5), datetime_entry('at', new DateTimeImmutable('2025-01-01 10:00:00.5', new DateTimeZone('UTC'))), uuid_entry('u', '01234567-89ab-4def-8123-456789abcdef'), list_entry('nums', [1, 2, 3], type_list(type_integer()))),
    row(int_entry('id', 2), str_entry('name', null), float_entry('price', -0.25), datetime_entry('at', new DateTimeImmutable('1999-12-31 23:59:59.999999', new DateTimeZone('Europe/Warsaw'))), uuid_entry('u', 'abcdef01-2345-4678-9abc-def012345678'), list_entry('nums', [], type_list(type_integer()))),
    row(int_entry('id', 3), str_entry('name', 'c'), float_entry('price', 99.0), datetime_entry('at', new DateTimeImmutable('2030-06-15 08:30:00', new DateTimeZone('America/New_York'))), uuid_entry('u', '11111111-2222-4333-8444-555566667777'), list_entry('nums', [-1, 0, PHP_INT_MAX], type_list(type_integer()))),
];

$schemaBody = FloeWriter::growSectionPlan(null, $sourceRows[0])->schemaBody;

$encoder = new RowsEncoder();
$encoder->schema($schemaBody);
$bodies = array_map(fn($row) => $encoder->row($row), $sourceRows);

$decoder = new RowsDecoder();
$decoder->schema($schemaBody);
$decoded = $decoder->rows($bodies);

var_dump($decoded instanceof Rows);
var_dump($decoded->count());

$reEncoder = new RowsEncoder();
$reEncoder->schema($schemaBody);
$reBodies = array_map(fn($row) => $reEncoder->row($row), $decoded->all());
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
