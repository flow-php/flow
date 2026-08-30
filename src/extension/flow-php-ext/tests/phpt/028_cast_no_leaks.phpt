--TEST--
repeated native cast does not leak memory, including the throwing fallback path
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Schema\Metadata;

use function Flow\ETL\DSL\{schema, int_schema, str_schema, float_schema, bool_schema, datetime_schema, date_schema, uuid_schema, list_schema, map_schema, structure_schema, json_schema, enum_schema};
use function Flow\Types\DSL\{structure_element, type_list, type_map, type_structure, type_integer, type_string, type_optional};

enum LeakSuit: string
{
    case Hearts = 'h';
}

$schema = schema(
    int_schema('id'),
    str_schema('name'),                       // non-nullable - null values force makeNullable
    float_schema('price'),
    bool_schema('active'),
    datetime_schema('created_at'),
    date_schema('day'),
    uuid_schema('uuid'),
    list_schema('ints', type_list(type_optional(type_integer()))),
    map_schema('metrics', type_map(type_string(), type_integer())),
    structure_schema('nested', type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)])),
    json_schema('json'),
    enum_schema('suit', LeakSuit::class),     // exotic - per-value PHP fallback
);

$batch = [];

for ($i = 1; $i <= 100; $i++) {
    $batch[] = new RawRowValues(
        [
            'id' => (string) $i,
            'name' => $i % 10 === 0 ? null : 'user_' . $i,     // exercises the makeNullable path
            'price' => \sprintf('%d.%02d', $i, $i % 100),
            'active' => $i % 2 === 0 ? 'yes' : 'off',
            'created_at' => \sprintf('2024-03-%02d 10:20:%02d', 1 + $i % 28, $i % 60),
            'day' => \sprintf('2024-03-%02d', 1 + $i % 28),
            'uuid' => \sprintf('550e8400-e29b-41d4-a716-%012d', $i),
            'ints' => [(string) $i, null, $i],
            'metrics' => ['cpu' => (string) $i, 'mem' => $i],
            'nested' => ['a' => (string) $i, 'b' => $i],
            'json' => '["a","b"]',
            'suit' => 'h',
        ],
        $i % 5 === 0 ? ['id' => Metadata::fromArray(['batch' => $i])] : [],   // exercises the clone + setMetadata path
    );
}

$throwingBatch = [new RawRowValues(['id' => 1]), new RawRowValues(['uuid' => 'not-a-uuid', 'id' => 2])];

$cycle = static function () use ($batch, $throwingBatch, $schema): void {
    $native = new RustRowHydratorNative();
    $native->cast($batch, $schema);

    try {
        $native->cast($throwingBatch, $schema);
    } catch (Throwable) {
        // the aborted batch must not leak its partially built rows
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
