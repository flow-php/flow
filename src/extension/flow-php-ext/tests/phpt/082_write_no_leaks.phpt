--TEST--
The native CSV and JSON writers leak neither PHP memory nor native allocations over 1 000 encodes, refusals included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\NativeCSVWriter;
use Flow\ETL\Adapter\JSON\NativeJsonWriter;
use Flow\ETL\Column\DefaultBackend;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_list;

$schema = all_types_schema();
$native = native_rows($schema, all_types_values());
$php = php_rows($schema, all_types_values());
// the string column holds bytes that are not UTF-8: JSON writes the batch without it
$jsonSchema = schema(...array_filter($schema->definitions(), static fn($definition): bool => $definition->entry()->name() !== 'string'));
$refusedSchema = schema(str_schema('s'), float_schema('f'), list_schema('l', type_list(type_float())));
$refused = native_rows($refusedSchema, [['s' => "\xff", 'f' => NAN, 'l' => [INF]]]);
$csv = new NativeCSVWriter(',', '"', '\\', "\n", 'Y-m-d\TH:i:s.uP T', 'Y-m-d');
$json = new NativeJsonWriter(JSON_THROW_ON_ERROR, 'Y-m-d\TH:i:s.uP T', 'Y-m-d');
$backend = new DefaultBackend();
$count = $native->count();
$cells = static function (array $names, string $cell) use ($count): array {
    return array_fill_keys($names, array_fill(0, $count, $cell));
};

$cycle = static function () use ($schema, $jsonSchema, $native, $php, $refused, $csv, $json, $cells): void {
    $csv->encode($native, $cells($csv->unrendered($schema), 'cell'));
    // PHP columns are adopted for the encode and freed after it
    $csv->encode($php, $cells($csv->unrendered($schema), 'cell'));
    $json->encode($native->project($jsonSchema), $cells($json->unrendered($jsonSchema), '"fragment"'), "\n");
    $json->encode($php->project($jsonSchema), $cells($json->unrendered($jsonSchema), '"fragment"'), ',');

    foreach ([
        static fn() => $csv->encode($refused, []),
        static fn() => $json->encode($refused, [], "\n"),
        static fn() => $csv->encode($native, []),
        static fn() => $json->encode($native, $cells($json->unrendered($schema), '"fragment"'), "\n"),
        static fn() => $json->encode($native->project($jsonSchema), ['json' => ['too', 'few']], "\n"),
    ] as $refusal) {
        try {
            $refusal();
            throw new LogicException('a refused batch was written');
        } catch (JsonException|Flow\ETL\Exception\RuntimeException|Flow\ETL\Exception\InvalidArgumentException) {
        }
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = $backend->allocatedBytes();

for ($i = 0; $i < 1_000; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump($backend->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
