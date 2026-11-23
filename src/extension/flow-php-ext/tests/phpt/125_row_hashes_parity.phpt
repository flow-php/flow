--TEST--
RustBackend::rowHashes() and groupedMultisetHashes() give the hashes PhpBackend gives: every tag, the canonical edge cases, a native map<string, integer> with the key "1", positioned and bare xml elements, columns from either backend, batches split differently, batches without columns, one group, absorbEarlier and the carry between lanes, md5 and sha256
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, Column, GroupedMultisetHashes, MultisetDigest, MultisetHashAlgorithm, PhpBackend, RustBackend};
use Flow\ETL\Values\Column\Php\MapColumn;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\{array_to_rows, datetime_schema, float_schema, int_schema, json_schema, map_schema, schema, str_schema, structure_schema, xml_element_schema};
use function Flow\Types\DSL\{type_integer, type_map, type_optional, type_string, type_structure, type_xml_element};

$php = new PhpBackend();
$rust = new RustBackend();

$edges = [
    schema(
        float_schema('float'),
        datetime_schema('utc', zone: 'UTC'),
        datetime_schema('warsaw', zone: 'Europe/Warsaw'),
        json_schema('json'),
        str_schema('text', nullable: true),
        int_schema('id', nullable: true),
        structure_schema('nested', type_structure([
            'b' => type_integer(),
            'a' => type_optional(type_structure(['x' => type_string()])),
        ])),
        xml_element_schema('element'),
    ),
    [
        ['float' => -0.0, 'utc' => '2026-01-01T00:00:00Z', 'warsaw' => '2026-01-01T01:00:00+01:00', 'json' => '{"a":1}', 'text' => null, 'id' => 1, 'nested' => ['b' => 1, 'a' => ['x' => 'y']], 'element' => type_xml_element()->cast('<div><a x="1"/></div>')->firstElementChild],
        ['float' => NAN, 'utc' => '1969-07-20T20:17:40.123456Z', 'warsaw' => '1969-07-20T20:17:40.123456Z', 'json' => '{"a": 1}', 'text' => '', 'id' => null, 'nested' => ['b' => 2, 'a' => null], 'element' => type_xml_element()->cast('<section><b/><a x="1"/></section>')->lastElementChild],
        ['float' => -NAN, 'utc' => '1970-01-01T00:00:00Z', 'warsaw' => '1970-01-01T00:00:00Z', 'json' => '[1]', 'text' => 'NaN', 'id' => null, 'nested' => ['b' => 3, 'a' => ['x' => '']], 'element' => (new DOMDocument())->createElement('a')],
        ['float' => 0.0, 'utc' => '2038-01-19T03:14:08.000001Z', 'warsaw' => '2038-01-19T03:14:08.000001Z', 'json' => '{}', 'text' => '1', 'id' => 1, 'nested' => ['b' => 1, 'a' => ['x' => 'y']], 'element' => type_xml_element()->cast('<div><a/></div>')->firstElementChild],
    ],
];
$cases = ['every tag' => [all_types_schema(), all_types_values()], 'edge cases' => $edges];

$rows = static fn(Backend $backend, Rows $source): Rows => $source->adoptedBy($backend);
$hashes = static function (Backend $backend, Rows $rows, int $batch, ?array $ids = null): array {
    $ids ??= array_fill(0, $rows->count(), 0);
    $grouped = $backend->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);

    foreach ($rows->chunks($batch) as $index => $part) {
        $grouped->add($part->columns(), array_to_rows(
            array_map(static fn(int $id): array => ['id' => $id], array_slice($ids, $index * $batch, $part->count())),
            schema(int_schema('id')),
            $backend,
        )->column('id'));
    }

    return $grouped->hashes()->physicals();
};

foreach ($cases as $label => [$schema, $values]) {
    $source = php_rows($schema, $values);
    $ids = array_map(static fn(int $row): int => $row % 2, array_keys($values));
    $expected = $php->rowHashes($source->columns(), $source->count())->physicals();
    $outcomes = [];

    foreach (['php columns' => $php, 'native columns' => $rust] as $from => $backend) {
        $built = $rows($backend, $source);
        $reversed = array_reverse($built->columns(), true);

        $outcomes[] = $rust->rowHashes($built->columns(), $built->count())->physicals() === $expected
            && $rust->rowHashes($reversed, $built->count())->physicals() === $expected
            && $php->rowHashes($built->columns(), $built->count())->physicals() === $expected
                ? 'same'
                : "MISMATCH rowHashes {$from}";

        foreach ([1, 2, count($values)] as $batch) {
            $outcomes[] = $hashes($rust, $built, $batch) === $hashes($php, $source, count($values))
                && $hashes($rust, $built, $batch, $ids) === $hashes($php, $source, count($values), $ids)
                    ? 'same'
                    : "MISMATCH multiset {$from} batch {$batch} " . json_encode($hashes($rust, $built, $batch)) . ' ' . json_encode($hashes($php, $source, count($values)));
        }
    }

    echo $label, ': ', implode(', ', array_unique($outcomes)), "\n";
}

$entries = array_to_rows([['k' => '1', 'v' => 1], ['k' => 'a', 'v' => 2]], schema(str_schema('k'), int_schema('v')), $php);
$map = new MapColumn(type_map(type_string(), type_integer()), [0, 1, 2], $entries->column('k'), $entries->column('v'), []);
$native = $rust->decode(map_schema('m', type_map(type_string(), type_integer())), $map->encode(), 2, 0);
echo 'native map with the key "1": ', $native instanceof Flow\ETL\Values\Column\RustColumn ? 'native' : 'php', ', ',
    $rust->rowHashes(['m' => $native], 2)->physicals() === $php->rowHashes(['m' => $map], 2)->physicals() ? 'same' : 'MISMATCH', "\n";

$source = php_rows(...$edges);
$groupIds = static fn(Backend $backend) => array_to_rows([['id' => 0], ['id' => 2], ['id' => 0], ['id' => 2]], schema(int_schema('id')), $backend)->column('id');
$fromPhp = $php->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$fromPhp->add($source->columns(), $groupIds($php));
$direct = $rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$direct->add($source->adoptedBy($rust)->columns(), $groupIds($rust));
$intoPhp = $rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$intoPhp->absorbEarlier($fromPhp, [0 => 0, 1 => 1, 2 => 2]);
$intoNative = $rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$intoNative->absorbEarlier($direct, [0 => 0, 1 => 1, 2 => 2]);
$phpInto = $php->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$phpInto->absorbEarlier($direct, [0 => 0, 1 => 1, 2 => 2]);
echo 'absorbEarlier of php, native and into php: ', $intoPhp->hashes()->physicals() === $direct->hashes()->physicals()
    && $intoNative->hashes()->physicals() === $direct->hashes()->physicals()
    && $phpInto->hashes()->physicals() === $direct->hashes()->physicals()
    && $intoPhp->hashes()->physicals() === $fromPhp->hashes()->physicals() ? 'same' : 'MISMATCH', "\n";
echo 'unseen group: ', $direct->hashes()->physicals()[1], "\n";
echo 'empty: ', json_encode($rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128)->hashes()->physicals()), "\n";

$withoutColumns = static function (Backend $backend): array {
    $one = $backend->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
    $one->add([], array_to_rows([['id' => 0], ['id' => 0], ['id' => 0]], schema(int_schema('id')), $backend)->column('id'));
    $grouped = $backend->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
    $grouped->add([], array_to_rows([['id' => 1], ['id' => 0], ['id' => 1]], schema(int_schema('id')), $backend)->column('id'));

    return [$one->hashes()->physicals(), $grouped->hashes()->physicals()];
};
echo 'batch without columns: ', $withoutColumns($rust) === $withoutColumns($php) ? 'same' : 'MISMATCH', ' ', json_encode($withoutColumns($rust)), "\n";

$earlier = static fn(string $hex): GroupedMultisetHashes => new class($hex) implements GroupedMultisetHashes {
    public function __construct(private readonly string $hex) {}
    public function add(array $columns, Column $ids): void {}
    public function hashes(): Column { return array_to_rows([['h' => $this->hex]], schema(str_schema('h')), new PhpBackend())->column('h'); }
    public function absorbEarlier(GroupedMultisetHashes $earlier, array $into): void {}
};
$carry = $rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
$carry->absorbEarlier($earlier((new MultisetDigest(0xFFFFFFFF, PHP_INT_MAX))->hex()), [0 => 0]);
$carry->absorbEarlier($earlier((new MultisetDigest(0xFFFFFFFF, 1))->hex()), [0 => 0]);
echo 'carry: ', $carry->hashes()->physicals()[0], "\n";

foreach (['uppercase' => '0000000000000000000000000000000A', 'sign' => '+0000000000000000000000000000000'] as $label => $hex) {
    foreach (['php' => $php, 'rust' => $rust] as $side => $backend) {
        try {
            $backend->groupedMultisetHashes(MultisetHashAlgorithm::xxh128)->absorbEarlier($earlier($hex), [0 => 0]);
            echo "{$label} into {$side}: accepted\n";
        } catch (Throwable $e) {
            echo "{$label} into {$side}: ", $e::class, ': ', $e->getMessage(), "\n";
        }
    }
}

try {
    $rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128)->absorbEarlier($rust->groupedMultisetHashes(MultisetHashAlgorithm::xxh128), [3 => 0]);
} catch (Throwable $e) {
    echo 'missing earlier group: ', $e::class, ': ', $e->getMessage(), "\n";
}

$only = array_to_rows([['a' => null]], schema(int_schema('a', true)), $rust);
echo 'only nulls: ', $rust->rowHashes($only->columns(), 1)->physicals()[0], "\n";
echo 'no columns: ', json_encode($rust->rowHashes([], 2)->physicals()), "\n";

$small = array_to_rows([['a' => 1], ['a' => 2], ['a' => 1]], schema(int_schema('a')), $php);

foreach ([MultisetHashAlgorithm::md5, MultisetHashAlgorithm::sha256] as $algorithm) {
    $of = static function (Backend $backend) use ($small, $algorithm, $rows): array {
        $grouped = $backend->groupedMultisetHashes($algorithm);
        $grouped->add($rows($backend, $small)->columns(), array_to_rows([['id' => 0], ['id' => 1], ['id' => 0]], schema(int_schema('id')), $backend)->column('id'));

        return $grouped->hashes()->physicals();
    };
    echo $algorithm->value, ': ', $of($rust) === $of($php) ? 'same' : 'MISMATCH', ' ', json_encode($of($rust)), "\n";
}
?>
--EXPECT--
every tag: same
edge cases: same
native map with the key "1": native, same
absorbEarlier of php, native and into php: same
unseen group: 00000000000000000000000000000000
empty: []
batch without columns: same [["ccfe147903d6ca882005496cd3a7dc7d"],["99aa06d3014798d86001c324468d497f","33540da6028f31b0c00386488d1a92fe"]]
carry: 800000000000000000000001fffffffe
uppercase into php: Flow\ETL\Foundation\Exception\InvalidArgumentException: A multiset digest is 32 lowercase hex characters, high lane first, given: "0000000000000000000000000000000A"
uppercase into rust: Flow\ETL\Foundation\Exception\InvalidArgumentException: A multiset digest is 32 lowercase hex characters, high lane first, given: "0000000000000000000000000000000A"
sign into php: Flow\ETL\Foundation\Exception\InvalidArgumentException: A multiset digest is 32 lowercase hex characters, high lane first, given: "+0000000000000000000000000000000"
sign into rust: Flow\ETL\Foundation\Exception\InvalidArgumentException: A multiset digest is 32 lowercase hex characters, high lane first, given: "+0000000000000000000000000000000"
missing earlier group: Flow\ETL\Foundation\Exception\InvalidArgumentException: Earlier group 3 has no hash
only nulls: 99aa06d3014798d86001c324468d497f
no columns: ["99aa06d3014798d86001c324468d497f","99aa06d3014798d86001c324468d497f"]
md5: same ["53b117ed69552c4ee4c638e6c72a526a","8c589f902ff9330489f46e8138ef3fff"]
sha256: same ["8f55cd2c66338b181c9f478cff77d05c","7c396438b2b1506bc82ea94d4ad238cd"]
