--TEST--
the native fold widens every pair of CSV leaf types exactly like TypeWidener
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustColumnFoldNative;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\Types\Type\TypeWidener;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

// one cell per leaf; null is an empty cell read with emptyToNull
$leaves = [
    'null' => [null, type_null()],
    'string' => ['x', type_string()],
    'json' => ['{"a":1}', type_json()],
    'uuid' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479', type_uuid()],
    'float' => ['1.5', type_float()],
    'integer' => ['1', type_integer()],
    'datetime' => ['2024-01-01 10:00:00', type_datetime()],
    'date' => ['2024-01-01', type_date()],
    'boolean' => ['true', type_boolean()],
    'timezone' => ['UTC', type_time_zone()],
];
$candidates = array_map(static fn($type): string => $type->toString(), InferredTypes::default()->toArray());
$widener = new TypeWidener();
$pairs = 0;
$mismatches = 0;

foreach ($leaves as $left => [$leftCell, $leftType]) {
    foreach ($leaves as $right => [$rightCell, $rightType]) {
        $csv = fopen('php://memory', 'rb+');
        fputcsv($csv, ['c'], ',', '"', '\\');
        fputcsv($csv, [$leftCell ?? ''], ',', '"', '\\');
        fputcsv($csv, [$rightCell ?? ''], ',', '"', '\\');
        rewind($csv);

        $reader = new RustCSVReaderNative(',', '"', '\\', true, true, true);
        $reader->feed((string) stream_get_contents($csv));
        $reader->finish();
        $fold = new RustColumnFoldNative([], $candidates);
        $reader->fold($fold, -1);

        $expected = $widener->widen($leftType, $rightType)->toString();
        $actual = $fold->types()['c'];
        $pairs++;

        if ($expected !== $actual) {
            $mismatches++;
            echo "{$left} + {$right}: TypeWidener={$expected} native={$actual}\n";
        }
    }
}

echo "{$pairs} pairs, {$mismatches} mismatches\n";
?>
--EXPECT--
100 pairs, 0 mismatches
