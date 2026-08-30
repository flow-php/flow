--TEST--
RustFloeEncoderNative frame bodies round-trip through the RawRowValues pipeline
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_uuid;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\RustFloeEncoderNative;

$schema = schema(
    int_schema('id'),
    str_schema('name', nullable: true),
    float_schema('price'),
    datetime_schema('at'),
    uuid_schema('u'),
    list_schema('nums', type_list(type_integer())),
);

$sourceRows = [
    row(['id' => 1, 'name' => 'a', 'price' => 1.5, 'at' => new DateTimeImmutable('2025-01-01 10:00:00.5', new DateTimeZone('UTC')), 'u' => type_uuid()->cast('01234567-89ab-4def-8123-456789abcdef'), 'nums' => [1, 2, 3]]),
    row(['id' => 2, 'name' => null, 'price' => -0.25, 'at' => new DateTimeImmutable('1999-12-31 23:59:59.999999', new DateTimeZone('Europe/Warsaw')), 'u' => type_uuid()->cast('abcdef01-2345-4678-9abc-def012345678'), 'nums' => []]),
    row(['id' => 3, 'name' => 'c', 'price' => 99.0, 'at' => new DateTimeImmutable('2030-06-15 08:30:00', new DateTimeZone('America/New_York')), 'u' => type_uuid()->cast('11111111-2222-4333-8444-555566667777'), 'nums' => [-1, 0, PHP_INT_MAX]]),
];

$schemaBody = json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
$hydrator = new PhpRowHydrator();

$encoder = new RustFloeEncoderNative();
$bodies = $encoder->encode($hydrator->dehydrate(new Rows($schema, ...$sourceRows)), $schemaBody);

$decoded = $hydrator->hydrate((new RustFloeEncoderNative())->decode($bodies, $schemaBody), schema_from_json($schemaBody));

var_dump($decoded instanceof Rows);
var_dump($decoded->count());

$reEncoder = new RustFloeEncoderNative();
$reBodies = $reEncoder->encode($hydrator->dehydrate($decoded), $schemaBody);
var_dump($bodies === $reBodies);

$frames = array_merge(
    [['type' => SCHEMA_ENTRY, 'body' => $schemaBody]],
    array_map(fn($body) => ['type' => Format::FRAME_ROW, 'body' => $body], $bodies),
);
assert_rows_identical(php_decode_frames($frames), $decoded->all());

var_dump($decoded->all()[1]->get('name'));
var_dump($decoded->all()[2]->get('nums'));
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
