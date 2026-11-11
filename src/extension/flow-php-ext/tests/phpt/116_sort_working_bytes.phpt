--TEST--
RustBackend::sortWorkingBytes() covers the native copy a native sort keeps, and what a sort handed to PhpBackend allocates: PhpBackend's heap plus the native copy, for list and struct keys
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{PhpBackend, RustBackend};
use Flow\ETL\Values\Sort\SortColumn;

use function Flow\ETL\DSL\{array_to_rows, float_schema, int_schema, list_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

$rust = new RustBackend();
$php = new PhpBackend();
$data = array_map(static fn(int $id): array => [
    'id' => ($id * 7919) % 10_000,
    'f' => $id / 7,
    'k' => 'key-' . ($id % 97),
    'l' => [$id],
], range(1, 10_000));
$schema = schema(int_schema('id'), float_schema('f'), str_schema('k'), list_schema('l', type_list(type_integer())));

$native = array_to_rows($data, $schema, $rust);
$keys = [new SortColumn('f'), new SortColumn('id')];
$before = $rust->allocatedBytes();
$sorted = $rust->sort($native, $keys);
$kept = $rust->allocatedBytes() - $before;
$estimate = $rust->sortWorkingBytes($native, $keys);
var_dump($estimate >= $kept, $estimate < 3 * $kept);

function handed_off_covered(RustBackend $rust, Flow\ETL\Values\Rows $rows, array $keys): bool
{
    gc_collect_cycles();
    $estimate = $rust->sortWorkingBytes($rows, $keys);
    $heap = memory_get_usage();
    $native = $rust->allocatedBytes();
    memory_reset_peak_usage();
    $sorted = $rust->sort($rows, $keys);
    $allocated = (memory_get_peak_usage() - $heap) + ($rust->allocatedBytes() - $native);

    return $estimate >= $allocated && $estimate < 3 * $allocated;
}

var_dump(handed_off_covered($rust, $native, [new SortColumn('l')]));
var_dump(handed_off_covered($rust, array_to_rows(
    array_map(static fn(int $id): array => ['s' => ['a' => $id % 97, 'b' => str_repeat('z', 1000) . $id], 'id' => $id], range(1, 10_000)),
    schema(structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()])), int_schema('id')),
    $rust,
), [new SortColumn('s')]));

$phpRows = array_to_rows($data, $schema, $php);
var_dump($rust->sortWorkingBytes($phpRows, $keys) === $php->sortWorkingBytes($phpRows, $keys));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
