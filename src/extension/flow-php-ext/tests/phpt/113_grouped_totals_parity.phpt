--TEST--
RustBackend::groupedTotals() keeps every group's count, sum and partial exactly as PhpBackend::groupedTotals(): int overflow, float sums in row order across many groups, NaN and infinities, numeric strings, nulls, groups first seen in later batches, ids from either backend
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Tests\Context\GroupedTotalsContext;
use Flow\ETL\Values\Column\{PhpBackend, RustBackend};

use function Flow\ETL\DSL\{float_schema, int_schema, str_schema};

$state = 7;
$random = static function (int $modulo) use (&$state): int {
    $state = ($state * 1_103_515_245 + 12_345) % 2_147_483_648;

    return $state % $modulo;
};
$manyGroups = [];
for ($batch = 0; $batch < 6; $batch++) {
    $values = [];
    $ids = [];
    for ($row = 0; $row < 700; $row++) {
        $values[] = $random(5) === 0 ? 1e16 : $random(1000) / 7 - 70;
        $ids[] = $random(50 + $batch * 40);
    }
    $manyGroups[] = [$values, $ids];
}

$cases = [
    'ints with overflow' => [int_schema('v', nullable: true), [[[PHP_INT_MAX, 1, 5, -3, PHP_INT_MIN, -1, null], [0, 0, 1, 1, 2, 2, 4]], [[1, 2, PHP_INT_MAX], [0, 3, 3]]]],
    'floats in row order across many groups' => [float_schema('v'), $manyGroups],
    'nan and infinities' => [float_schema('v', nullable: true), [[[NAN, 1.0, INF, -INF, INF, -0.0, null], [0, 0, 1, 1, 2, 3, 5]]]],
    'numeric strings' => [str_schema('v', nullable: true), [[['a', null, '3.5', ' 2', '1e3', '', '9223372036854775807'], [0, 0, 0, 1, 1, 2, 3]]]],
    'groups first seen later' => [int_schema('v'), [[[1], [0]], [[2, 3], [1, 0]], [[4], [7]]]],
];

foreach ($cases as $label => [$definition, $batches]) {
    $outcome = static function ($backend) use ($definition, $batches): string {
        $totals = (new GroupedTotalsContext($backend, $definition))->grouped($batches);

        return var_export([$totals->sums()->values(), $totals->partials()], true) . ' ' . get_class($totals->sums());
    };
    $php = $outcome(new PhpBackend());
    $rust = $outcome(new RustBackend());

    [$phpValues] = explode(' Flow', $php);
    [$rustValues, $rustClass] = explode(' Flow', $rust);

    echo $label, ': ', $phpValues === $rustValues ? 'same, Flow' . $rustClass : "MISMATCH\n" . $php . "\n" . $rust, ', ', substr_count($rust, "'count' =>"), " groups\n";
}

$absorbed = static function ($earlierBackend, $backend): string {
    $context = new GroupedTotalsContext($backend, float_schema('v', nullable: true));
    $later = $context->grouped([[[0.25, 1.0, 7.5, null, PHP_INT_MAX], [0, 1, 1, 2, 4]]]);
    $later->absorbEarlier((new GroupedTotalsContext($earlierBackend, float_schema('v', nullable: true)))->grouped([[[1e16, 0.5, 2.0, -3.25, null], [0, 0, 1, 2, 3]]]), [0 => 1, 1 => 0, 2 => 2, 3 => 6]);

    return var_export($later->partials(), true);
};
$expected = $absorbed(new PhpBackend(), new PhpBackend());
echo 'absorbing earlier totals: rust into rust ', $absorbed(new RustBackend(), new RustBackend()) === $expected ? 'same' : 'MISMATCH', ', php into rust ', $absorbed(new PhpBackend(), new RustBackend()) === $expected ? 'same' : 'MISMATCH', "\n";

foreach (['php' => new PhpBackend(), 'rust' => new RustBackend()] as $label => $backend) {
    try {
        $backend->groupedTotals()->absorbEarlier($backend->groupedTotals(), [3 => 0]);
        echo 'absorbing an unknown earlier group, ', $label, ": no exception\n";
    } catch (Throwable $e) {
        echo 'absorbing an unknown earlier group, ', $label, ': ', $e::class, ' ', $e->getMessage(), "\n";
    }
}

$mixed = (new RustBackend())->groupedTotals();
$mixed->add((new GroupedTotalsContext(new PhpBackend(), int_schema('v')))->column(int_schema('v'), [5, 6]), (new GroupedTotalsContext(new PhpBackend(), int_schema('id')))->column(int_schema('id'), [1, 1]));
echo 'php columns into rust totals: ', json_encode(array_column($mixed->partials(), 'count')), ' ', json_encode($mixed->sums()->values()), ' ', get_class($mixed->sums()), "\n";
?>
--EXPECT--
ints with overflow: same, Flow\ETL\Values\Column\RustColumn, 5 groups
floats in row order across many groups: same, Flow\ETL\Values\Column\RustColumn, 250 groups
nan and infinities: same, Flow\ETL\Values\Column\RustColumn, 6 groups
numeric strings: same, Flow\ETL\Values\Column\RustColumn, 4 groups
groups first seen later: same, Flow\ETL\Values\Column\RustColumn, 8 groups
absorbing earlier totals: rust into rust same, php into rust same
absorbing an unknown earlier group, php: Flow\ETL\Foundation\Exception\InvalidArgumentException Earlier group 3 has no totals
absorbing an unknown earlier group, rust: Flow\ETL\Foundation\Exception\InvalidArgumentException Earlier group 3 has no totals
php columns into rust totals: [0,2] [null,11] Flow\ETL\Values\Column\RustColumn
