--TEST--
RustBackend::partition() puts every row in the bucket PhpBackend::partition() gives it, in row order: text keys natively, the other types through KeyEncoder, composite keys, columns from either backend, offsets 0 and 8, 1, 4 and 16 buckets
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, bool_schema, float_schema, int_schema, list_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

$cases = [
    'nullable strings' => [[str_schema('k', nullable: true)], [['a'], [null], ['b'], [''], [null], ['a'], ['0']]],
    'ints' => [[int_schema('k')], array_map(static fn(int $i): array => [$i % 11 - 5], range(0, 40))],
    'booleans' => [[bool_schema('k', nullable: true)], [[true], [false], [null], [true]]],
    'floats' => [[float_schema('k')], [[0.0], [-0.0], [1.5], [NAN], [1.5]]],
    'lists with nulls' => [[list_schema('k', type_list(type_integer()), nullable: true)], [[[1]], [null], [[]], [[2, 3]], [[1]]]],
    'structures with nulls' => [[structure_schema('k', type_structure(['a' => type_string()]), nullable: true)], [[['a' => 'x']], [null], [['a' => 'y']], [['a' => 'x']]]],
    'only nulls' => [[str_schema('k', nullable: true)], [[null], [null]]],
    'a composite key' => [[int_schema('k'), str_schema('s')], [[1, 'a'], [1, 'b'], [2, 'a'], [1, 'a']]],
];

$partitioned = static function (Backend $partitioner, Backend $columns, array $definitions, array $values, int $buckets, int $offset): array {
    $names = array_map(static fn($definition): string => $definition->name(), $definitions);
    $rows = array_to_rows(
        array_map(static fn(array $row, int $i): array => [...array_combine($names, $row), 'i' => $i], $values, array_keys($values)),
        schema(...$definitions, ...[int_schema('i')]),
        $columns,
    );
    $parts = $partitioner->partition($rows, array_map(static fn(string $name) => $rows->column($name), $names), $buckets, $offset);

    return array_map(static fn($part): array => $part->column('i')->values(), $parts);
};
$php = new PhpBackend();
$rust = new RustBackend();

foreach ($cases as $label => [$definitions, $values]) {
    $outcomes = [];

    foreach ([1, 4, 16] as $buckets) {
        foreach ([0, 8] as $offset) {
            $expected = $partitioned($php, $php, $definitions, $values, $buckets, $offset);
            $outcomes[] = $partitioned($rust, $rust, $definitions, $values, $buckets, $offset) === $expected
                && $partitioned($rust, $php, $definitions, $values, $buckets, $offset) === $expected
                    ? 'same'
                    : "MISMATCH {$buckets}/{$offset} " . json_encode($partitioned($rust, $rust, $definitions, $values, $buckets, $offset)) . ' ' . json_encode($expected);
        }
    }

    echo $label, ': ', implode(', ', array_unique($outcomes)), "\n";
}

$empty = array_to_rows([], schema(int_schema('k')), $rust);
echo 'empty batch: ', json_encode($rust->partition($empty, [$empty->column('k')], 4, 0)), "\n";
$one = array_to_rows([['k' => 1], ['k' => 2]], schema(int_schema('k')), $rust);
echo 'one bucket is the batch itself: ', var_export($rust->partition($one, [$one->column('k')], 1, 0)[0] === $one, true), "\n";

foreach (['a bucket count of 0' => [0, 0], 'a hash offset of 25' => [4, 25], 'a hash offset of -1' => [4, -1]] as $label => [$buckets, $offset]) {
    $refusal = static function (Backend $backend) use ($empty, $buckets, $offset): string {
        try {
            $backend->partition($empty, [$empty->column('k')], $buckets, $offset);

            return 'no exception';
        } catch (Throwable $e) {
            return $e::class . ' ' . $e->getMessage();
        }
    };

    echo $label, ': ', $refusal($rust) === $refusal($php) ? 'same ' . $refusal($rust) : 'MISMATCH ' . $refusal($rust) . ' | ' . $refusal($php), "\n";
}
?>
--EXPECT--
nullable strings: same
ints: same
booleans: same
floats: same
lists with nulls: same
structures with nulls: same
only nulls: same
a composite key: same
empty batch: []
one bucket is the batch itself: true
a bucket count of 0: same Flow\ETL\Foundation\Exception\InvalidArgumentException Buckets count must be greater than 0, given: 0
a hash offset of 25: same Flow\ETL\Foundation\Exception\InvalidArgumentException Hash offset must be between 0 and 24, given: 25
a hash offset of -1: same Flow\ETL\Foundation\Exception\InvalidArgumentException Hash offset must be between 0 and 24, given: -1
