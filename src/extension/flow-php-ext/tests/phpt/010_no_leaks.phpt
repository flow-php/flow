--TEST--
repeated frame-body encode and decode do not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

$schema = schema(
    int_schema('id'),
    str_schema('name', nullable: true),
    float_schema('price'),
    bool_schema('active'),
    datetime_schema('created_at'),
    time_schema('duration'),
    uuid_schema('uuid'),
    map_schema('metrics', type_map(type_string(), type_float())),
    structure_schema('nested', type_structure(['a' => type_integer(), 'tags' => type_list(type_string())])),
    xml_schema('doc'),
    json_schema('json'),
);

$build = static function (int $i): Flow\ETL\Row {
    return row([
        'id' => $i,
        'name' => $i % 10 === 0 ? null : 'user_' . $i,
        'price' => $i / 100.0,
        'active' => $i % 3 === 0,
        'created_at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw')),
        'duration' => new DateInterval('PT1H2M3S'),
        'uuid' => type_uuid()->cast('01234567-89ab-4def-8123-456789abcdef'),
        'metrics' => ['cpu' => 0.5, 'mem' => 0.25],
        'nested' => ['a' => $i, 'tags' => ['x']],
        'doc' => type_xml()->cast('<root><item>' . $i . '</item></root>'),
        'json' => type_json()->cast([$i, ['a' => true]]),
    ]);
};

$sourceRows = array_map($build, range(1, 100));
$frames = php_frames(rows($schema, ...$sourceRows));

$schemaBody = null;
$rowBodies = [];

foreach ($frames as $frame) {
    if ($frame['type'] === SCHEMA_ENTRY) {
        $schemaBody = $frame['body'];
    } else {
        $rowBodies[] = $frame['body'];
    }
}

$decodeSchema = schema_from_json($schemaBody);

$cycle = static function () use ($schemaBody, $decodeSchema, $rowBodies, $schema, $sourceRows): void {
    $decoder = new RustFloeEncoderNative();
    $hydrator = new PhpRowHydrator();
    $hydrator->hydrate($decoder->decode($rowBodies, $schemaBody), $decodeSchema);

    foreach ($rowBodies as $body) {
        $hydrator->hydrate($decoder->decode([$body], $schemaBody), $decodeSchema);
    }

    $encoder = new RustFloeEncoderNative();
    $encoder->encode($hydrator->dehydrate(new Rows($schema, ...$sourceRows)), $schemaBody);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
// memory_get_usage(false) counts emalloc'd bytes exactly - any per-call leak
// (even one zval) shows up here; (true) only tracks arena chunks and grows
// from GC-buffer expansion without any leak.
$baseline = memory_get_usage(false);

for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
?>
--EXPECT--
bool(true)
