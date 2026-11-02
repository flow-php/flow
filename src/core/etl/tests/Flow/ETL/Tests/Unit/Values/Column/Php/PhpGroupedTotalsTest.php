<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Php;

use Flow\ETL\Functions\NumericTotal;
use Flow\ETL\Tests\Context\GroupedTotalsContext;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Schema\Definition;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;

use const INF;
use const NAN;
use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class PhpGroupedTotalsTest extends TestCase
{
    /**
     * @return Generator<string, array{Definition<mixed>, list<array{0: list<mixed>, 1: list<int>}>}>
     */
    public static function batches(): Generator
    {
        yield 'ints, an overflow promotes to float' => [
            int_schema('v', nullable: true),
            [
                [[PHP_INT_MAX, 1, 5, -3, PHP_INT_MIN, -1, null], [0, 0, 1, 1, 2, 2, 4]],
                [[1, 2, PHP_INT_MAX], [0, 3, 3]],
            ],
        ];
        yield 'floats summed in row order with compensation' => [
            float_schema('v'),
            [
                [
                    array_map(static fn(int $i): float => ($i % 3) === 0 ? 1e16 : 0.1 * $i, range(0, 999)),
                    array_map(static fn(int $i): int => ($i * 7) % 37, range(0, 999)),
                ],
                [
                    array_map(static fn(int $i): float => -0.3 * $i, range(0, 499)),
                    array_map(static fn(int $i): int => ($i * 11) % 41, range(0, 499)),
                ],
            ],
        ];
        yield 'ints then floats in one group' => [float_schema('v'), [[[1.0, 0.5, 2.0, -0.0], [0, 0, 0, 1]]]];
        yield 'nan and infinities' => [float_schema('v'), [[[NAN, 1.0, INF, -INF, INF, 1.5], [0, 0, 1, 1, 2, 2]]]];
        yield 'numeric strings count, other strings and nulls do not' => [
            str_schema('v', nullable: true),
            [
                [
                    ['a', null, '3.5', '2', '1e3', ''],
                    [0,   0,    0,     1,   1,     2],
                ],
            ],
        ];
        yield 'a group with no numeric value' => [int_schema('v', nullable: true), [[[null, 1, null], [2, 0, 2]]]];
        yield 'new groups in a later batch' => [int_schema('v'), [[[1], [0]], [[2, 3], [1, 0]], [[4], [5]]]];
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<array{0: list<mixed>, 1: list<int>}> $batches
     */
    #[DataProvider('batches')]
    public function test_totals_by_group_equal_a_numeric_total_per_group(Definition $definition, array $batches): void
    {
        $context = new GroupedTotalsContext(new PhpBackend(), $definition);
        $grouped = $context->grouped($batches);
        $perGroup = $context->perGroup($batches);

        static::assertSame(
            var_export(array_map(static fn(NumericTotal $total): array => $total->partial(), $perGroup), true),
            var_export(array_slice($grouped->partials(), 0, count($perGroup)), true),
        );
        static::assertSame(
            array_map(static fn(NumericTotal $total): int => $total->count(), $perGroup),
            array_slice(array_column($grouped->partials(), 'count'), 0, count($perGroup)),
        );
        static::assertSame(
            var_export(array_map(static fn(NumericTotal $total): ?float => $total->sum() === null
                ? null
                : (float) $total->sum(), $perGroup), true),
            var_export(array_slice($grouped->sums()->values(), 0, count($perGroup)), true),
        );
    }

    public function test_a_group_id_without_rows_has_no_count_and_no_sum(): void
    {
        $grouped = (new GroupedTotalsContext(new PhpBackend(), int_schema('v')))->grouped([[[7], [2]]]);

        static::assertSame([0, 0, 1], array_column($grouped->partials(), 'count'));
        static::assertSame([null, null, 7.0], $grouped->sums()->values());
    }

    public function test_absorbing_earlier_totals_equals_merging_a_numeric_total_into_the_earlier_one(): void
    {
        $context = new GroupedTotalsContext(new PhpBackend(), float_schema('v', nullable: true));
        $earlierBatches = [[[1e16, 0.5, 2.0, -3.25, null], [0, 0, 1, 2, 3]]];
        $laterBatches = [[
            [0.25, 1.0, 7.5, null],
            [0,    1,   1,   2],
        ]];
        $into = [0 => 1, 1 => 0, 2 => 2, 3 => 5];

        $later = $context->grouped($laterBatches);
        $later->absorbEarlier($context->grouped($earlierBatches), $into);

        $earlierTotals = $context->perGroup($earlierBatches);
        $laterTotals = $context->perGroup($laterBatches);
        $expected = [];

        foreach ([0, 1, 2, 3, 4, 5] as $id) {
            $earlierId = array_search($id, $into, true);
            $total = $earlierId === false ? $laterTotals[$id] ?? new NumericTotal() : $earlierTotals[$earlierId];

            if ($earlierId !== false && array_key_exists($id, $laterTotals)) {
                $total->merge($laterTotals[$id]->partial());
            }

            $expected[] = $total->partial();
        }

        static::assertSame(var_export($expected, true), var_export($later->partials(), true));
    }
}
