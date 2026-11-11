--TEST--
RustBackend::sortWorkingBytes() of a slice counts the slice, not the buffer it shares: batches cut from one block, lists and maps included, sum to that block's figure
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Sort\SortColumn;

use function Flow\ETL\DSL\{array_to_rows, int_schema, list_schema, map_schema, schema, str_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string};

$backend = new RustBackend();
$keys = [new SortColumn('id')];
$block = $backend->sort(array_to_rows(
    array_map(static fn(int $id): array => [
        'id' => ($id * 7919) % 50_000,
        'k' => 'key-' . ($id % 97),
        'l' => [$id, $id % 7, 3],
        'm' => ['a' => $id],
    ], range(1, 50_000)),
    schema(
        int_schema('id'),
        str_schema('k'),
        list_schema('l', type_list(type_integer())),
        map_schema('m', type_map(type_string(), type_integer())),
    ),
    $backend,
), $keys);
$sliced = 0;

foreach ($block->chunks(1000) as $batch) {
    $sliced += $backend->sortWorkingBytes($batch, $keys);
}

var_dump($sliced <= 1.1 * $backend->sortWorkingBytes($block, $keys));
?>
--EXPECT--
bool(true)
