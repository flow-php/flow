--TEST--
RustBackend::groupedCounts() counts every group's non-null values exactly as PhpBackend::groupedCounts(): scalars, lists and structures with nulls, columns holding only nulls, groups first seen in later batches, columns from either backend, absorbing earlier counts
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, int_schema, list_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

$cases = [
    'nullable strings' => [str_schema('v', nullable: true), [[['a', null, 'b', '', null], [0, 1, 0, 2, 2]], [[null, 'c'], [3, 1]]]],
    'ints without nulls' => [int_schema('v'), [[[1, 2, 3], [0, 0, 1]]]],
    'lists with nulls' => [list_schema('v', type_list(type_integer()), nullable: true), [[[[1], null, [], [2, 3]], [0, 0, 1, 1]]]],
    'structures with nulls' => [structure_schema('v', type_structure(['a' => type_string()]), nullable: true), [[[['a' => 'x'], null, ['a' => 'y']], [1, 0, 1]]]],
    'only nulls' => [str_schema('v', nullable: true), [[[null, null], [0, 2]]]],
    'groups first seen later' => [int_schema('v'), [[[1], [0]], [[2, 3], [1, 0]], [[4], [7]]]],
];

$counted = static function (Backend $counter, Backend $columns, $definition, array $batches) {
    $counts = $counter->groupedCounts();

    foreach ($batches as [$values, $ids]) {
        $rows = array_to_rows(
            array_map(static fn($value, $id) => ['v' => $value, 'id' => $id], $values, $ids),
            schema($definition, int_schema('id')),
            $columns,
        );
        $counts->add($rows->column('v'), $rows->column('id'));
    }

    return $counts;
};
$php = new PhpBackend();
$rust = new RustBackend();

foreach ($cases as $label => [$definition, $batches]) {
    $expected = $counted($php, $php, $definition, $batches)->partials();
    $native = $counted($rust, $rust, $definition, $batches);

    echo $label, ': ', var_export($native->partials(), true) === var_export($expected, true) ? 'same' : 'MISMATCH ' . json_encode($native->partials()) . ' ' . json_encode($expected),
        ', php columns ', $counted($rust, $php, $definition, $batches)->partials() === $expected ? 'same' : 'MISMATCH',
        ', ', json_encode($native->counts()->values()), ' ', get_class($native->counts()), "\n";
}

$absorbed = static function (Backend $earlierBackend, Backend $backend, array $into) use ($counted, $cases): array {
    [$definition, $batches] = $cases['nullable strings'];
    $later = $counted($backend, $backend, $definition, [[['x', null, 'y'], [0, 1, 1]]]);
    $later->absorbEarlier($counted($earlierBackend, $earlierBackend, $definition, $batches), $into);

    return $later->partials();
};
$expected = $absorbed($php, $php, [0 => 1, 1 => 0, 3 => 5]);
echo 'absorbing earlier counts: ', json_encode($expected), ' rust into rust ', $absorbed($rust, $rust, [0 => 1, 1 => 0, 3 => 5]) === $expected ? 'same' : 'MISMATCH', ', php into rust ', $absorbed($php, $rust, [0 => 1, 1 => 0, 3 => 5]) === $expected ? 'same' : 'MISMATCH', "\n";

foreach (['php' => [$php, $php], 'rust' => [$rust, $rust], 'php into rust' => [$php, $rust]] as $label => [$earlierBackend, $backend]) {
    try {
        $absorbed($earlierBackend, $backend, [9 => 2]);
        echo $label, ": no exception\n";
    } catch (Throwable $e) {
        echo $label, ': ', $e::class, ' ', $e->getMessage(), "\n";
    }
}
?>
--EXPECT--
nullable strings: same, php columns same, [2,1,1,0] Flow\ETL\Values\Column\RustColumn
ints without nulls: same, php columns same, [2,1] Flow\ETL\Values\Column\RustColumn
lists with nulls: same, php columns same, [1,2] Flow\ETL\Values\Column\RustColumn
structures with nulls: same, php columns same, [0,2] Flow\ETL\Values\Column\RustColumn
only nulls: same, php columns same, [0,0,0] Flow\ETL\Values\Column\RustColumn
groups first seen later: same, php columns same, [2,1,0,0,0,0,0,1] Flow\ETL\Values\Column\RustColumn
absorbing earlier counts: [2,3,0,0,0,0] rust into rust same, php into rust same
php: Flow\ETL\Foundation\Exception\InvalidArgumentException Earlier group 9 has no count
rust: Flow\ETL\Foundation\Exception\InvalidArgumentException Earlier group 9 has no count
php into rust: Flow\ETL\Foundation\Exception\InvalidArgumentException Earlier group 9 has no count
