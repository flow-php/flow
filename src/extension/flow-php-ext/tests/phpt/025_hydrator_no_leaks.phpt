--TEST--
repeated native hydrate and dehydrate do not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Schema\Metadata;

use function Flow\ETL\DSL\{schema, int_schema, str_schema, float_schema, bool_schema, datetime_schema, time_schema, uuid_schema, list_schema, map_schema, structure_schema, json_schema};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_string, type_float, type_mixed};

$schema = schema(
    int_schema('id'),
    str_schema('name'),                       // non-nullable - null values force makeNullable
    float_schema('price'),
    bool_schema('active'),
    datetime_schema('created_at'),
    time_schema('duration'),
    uuid_schema('uuid'),
    list_schema('mixed', type_list(type_mixed())),
    map_schema('metrics', type_map(type_string(), type_float())),
    structure_schema('nested', type_structure(['a' => type_integer(), 'tags' => type_list(type_string())])),
    json_schema('json'),
);

$batch = [];

for ($i = 1; $i <= 100; $i++) {
    $batch[] = new RawRowValues(
        [
            'id' => $i,
            'name' => $i % 10 === 0 ? null : 'user_' . $i,     // exercises the makeNullable path
            'price' => $i / 100,
            'active' => $i % 3 === 0,
            'created_at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw')),
            'duration' => new DateInterval('PT1H2M3S'),
            'uuid' => new Flow\Types\Value\Uuid('01234567-89ab-4def-8123-456789abcdef'),
            'mixed' => [$i, 'x', null, ['k' => 1.5]],
            'metrics' => ['cpu' => 0.5, 'mem' => 0.25],
            'nested' => ['a' => $i, 'tags' => ['x']],
            'json' => Flow\Types\Value\Json::fromArray([$i, ['a' => true]]),
        ],
        $i % 5 === 0 ? ['id' => Metadata::fromArray(['batch' => $i])] : [],   // exercises the clone + setMetadata path
    );
}

$phpRows = (new PhpRowHydrator())->hydrate($batch, $schema);

$cycle = static function () use ($batch, $schema, $phpRows): void {
    $native = new RustRowHydratorNative();
    $rows = $native->hydrate($batch, $schema);
    $native->dehydrate($rows);
    $native->dehydrate($phpRows);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);

for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
?>
--EXPECT--
bool(true)
