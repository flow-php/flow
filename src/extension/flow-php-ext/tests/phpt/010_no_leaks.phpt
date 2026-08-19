--TEST--
repeated frame-body encode and decode do not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, bool_entry, datetime_entry, time_entry, uuid_entry, list_entry, map_entry, structure_entry, xml_entry, json_entry, schema_from_json};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_string, type_float};

$build = static function (int $i): Flow\ETL\Row {
    return row(
        int_entry('id', $i),
        str_entry('name', $i % 10 === 0 ? null : 'user_' . $i),
        float_entry('price', $i / 100),
        bool_entry('active', $i % 3 === 0),
        datetime_entry('created_at', new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw'))),
        time_entry('duration', new DateInterval('PT1H2M3S')),
        uuid_entry('uuid', '01234567-89ab-4def-8123-456789abcdef'),
        map_entry('metrics', ['cpu' => 0.5, 'mem' => 0.25], type_map(type_string(), type_float())),
        structure_entry('nested', ['a' => $i, 'tags' => ['x']], type_structure(['a' => type_integer(), 'tags' => type_list(type_string())])),
        xml_entry('doc', '<root><item>' . $i . '</item></root>'),
        json_entry('json', [$i, ['a' => true]]),
    );
};

$sourceRows = array_map($build, range(1, 100));
$frames = php_frames(rows(...$sourceRows));

$schemaBody = null;
$rowBodies = [];

foreach ($frames as $frame) {
    if ($frame['type'] === SCHEMA_ENTRY) {
        $schemaBody = $frame['body'];
    } else {
        $rowBodies[] = $frame['body'];
    }
}

$schema = schema_from_json($schemaBody);

$cycle = static function () use ($schemaBody, $schema, $rowBodies, $sourceRows): void {
    $decoder = new RustFloeEncoderNative();
    $hydrator = new PhpRowHydrator();
    $hydrator->hydrate($decoder->decode($rowBodies, $schemaBody), $schema);

    foreach ($rowBodies as $body) {
        $hydrator->hydrate($decoder->decode([$body], $schemaBody), $schema);
    }

    $encoder = new RustFloeEncoderNative();
    $encoder->encode($hydrator->dehydrate(new Rows(...$sourceRows)), $schemaBody);
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
