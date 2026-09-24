--TEST--
repeated fused decodeRows, refusals included, does not leak memory
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
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\Floe\RustFloeEncoderNative;
use Flow\ETL\Schema\Metadata;

$schema = schema(
    int_schema('id'),
    str_schema('name', nullable: true),       // the batch carries nulls, so the declaration admits them
    float_schema('price'),
    bool_schema('active'),
    datetime_schema('created_at'),
    time_schema('duration'),
    uuid_schema('uuid'),
    list_schema('tags', type_list(type_string())),
    map_schema('metrics', type_map(type_string(), type_float())),
    structure_schema('nested', type_structure(['a' => type_integer(), 'tags' => type_list(type_string())])),
    json_schema('json'),
);

$batch = [];

for ($i = 1; $i <= 100; $i++) {
    $batch[] = new RawRowValues(
        [
            'id' => $i,
            'name' => $i % 10 === 0 ? null : 'user_' . $i,     // null values under a nullable declaration
            'price' => (float) $i / 100,
            'active' => $i % 3 === 0,
            'created_at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw')),
            'duration' => new DateInterval('PT1H2M3S'),
            'uuid' => new Flow\Types\Value\Uuid('01234567-89ab-4def-8123-456789abcdef'),
            'tags' => ['x', 'user_' . $i],
            'metrics' => ['cpu' => 0.5, 'mem' => 0.25],
            'nested' => ['a' => $i, 'tags' => ['x']],
            'json' => Flow\Types\Value\Json::fromArray([$i, ['a' => true]]),
        ],
        $i % 5 === 0 ? ['id' => Metadata::fromArray(['batch' => $i])] : [],   // exercises the clone + setMetadata path
    );
}

$schemaBody = json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
// the per-row metadata diverges from the file schema, so the bodies carry VALUE_*_WITH_META flags
$bodies = (new RustFloeEncoderNative())->encode((new RustRowHydratorNative())->dehydrate((new PhpRowHydrator())->hydrate($batch, $schema)), $schemaBody, $schema);
$refusing = schema(int_schema('id'), int_schema('name', nullable: true));

$cycle = static function () use ($bodies, $schemaBody, $schema, $refusing): void {
    $native = new RustFloeEncoderNative();
    $native->decodeRows($bodies, $schemaBody, $schema);

    try {
        $native->decodeRows($bodies, $schemaBody, $refusing);
    } catch (Flow\ETL\Exception\SchemaMismatchException) {
    }
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
