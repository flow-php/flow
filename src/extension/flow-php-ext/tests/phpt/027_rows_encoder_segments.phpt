--TEST--
RowsEncoder::rows produces section segments identical to Flow\Floe\PhpRowFrameEncoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\PhpRowFrameEncoder;
use Flow\Floe\RowsEncoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, bool_entry, list_entry, map_entry, structure_entry};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_string, type_float};

$datasets = [
    'homogeneous' => rows(
        row(int_entry('id', 1), str_entry('name', null), float_entry('p', -1.5), bool_entry('a', true)),
        row(int_entry('id', 2), str_entry('name', 'x'), float_entry('p', 0.25), bool_entry('a', false)),
    ),
    'heterogeneous' => rows(
        row(int_entry('id', 1)),
        row(int_entry('id', 2), str_entry('n', 'x')),
        row(str_entry('n', 'y'), int_entry('id', 3)),
        row(int_entry('id', 4)),
    ),
    'parametric' => rows(
        row(int_entry('id', 1), list_entry('l', [1, 2], type_list(type_integer()))),
        row(int_entry('id', 2), map_entry('m', ['cpu' => 1.5], type_map(type_string(), type_float()))),
        row(int_entry('id', 3), structure_entry('st', ['a' => 1], type_structure(['a' => type_integer()]))),
    ),
    'numeric_string_names' => rows(
        row(int_entry('0', 1), str_entry('1', 'a')),
        row(int_entry('0', 2), str_entry('1', 'b')),
    ),
    'empty' => rows(),
];

function segment_dump(array $segments): array
{
    $dump = [];

    foreach ($segments as $segment) {
        if (!$segment instanceof \Flow\Floe\FrameSegment) {
            throw new RuntimeException('expected a FrameSegment, got ' . get_debug_type($segment));
        }

        $dump[] = [$segment->schemaBody, bin2hex($segment->frames), $segment->rowCount];
    }

    return $dump;
}

foreach ($datasets as $name => $dataset) {
    $php = segment_dump((new PhpRowFrameEncoder())->encode($dataset));
    $ext = segment_dump((new RowsEncoder())->rows($dataset));

    echo $name . ': ' . ($php === $ext ? 'identical' : 'MISMATCH') . ' (' . count($ext) . " segments)\n";
}

// state continuation: the second call continues the section left open by the first
$phpEncoder = new PhpRowFrameEncoder();
$extEncoder = new RowsEncoder();

$first = rows(row(int_entry('id', 1)));
$second = rows(row(int_entry('id', 2)));

$phpFirst = segment_dump($phpEncoder->encode($first));
$extFirst = segment_dump($extEncoder->rows($first));
$phpSecond = segment_dump($phpEncoder->encode($second));
$extSecond = segment_dump($extEncoder->rows($second));

echo 'continuation first: ' . ($phpFirst === $extFirst ? 'identical' : 'MISMATCH') . "\n";
echo 'continuation second: ' . ($phpSecond === $extSecond ? 'identical' : 'MISMATCH') . "\n";
echo 'continuation second schemaBody is null: ' . ($extSecond[0][0] === null ? 'yes' : 'no') . "\n";
?>
--EXPECT--
homogeneous: identical (1 segments)
heterogeneous: identical (2 segments)
parametric: identical (3 segments)
numeric_string_names: identical (1 segments)
empty: identical (0 segments)
continuation first: identical
continuation second: identical
continuation second schemaBody is null: yes
