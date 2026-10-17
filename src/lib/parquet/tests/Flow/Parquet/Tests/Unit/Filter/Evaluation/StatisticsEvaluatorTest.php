<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use Flow\Parquet\Filter\Truth;
use Flow\Parquet\Tests\Context\ZoneTruth;
use Flow\Parquet\Tests\Mother\ZoneMapMother;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

use const INF;

final class StatisticsEvaluatorTest extends TestCase
{
    public function test_comparison_positive_never_when_all_null_or_the_bounds_miss_the_interval(): void
    {
        $lt = parquet_column('i')->lessThan(5);

        static::assertSame(Truth::Never, ZoneTruth::of($lt, ['i' => ZoneMapMother::allNull()]));
        static::assertSame(Truth::Never, ZoneTruth::of($lt, ['i' => ZoneMapMother::withNulls(5, 9)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($lt, ['i' => ZoneMapMother::withNulls(4, 9)]));
    }

    public function test_comparison_positive_always_when_no_null_and_the_interval_holds_the_bounds(): void
    {
        $lt = parquet_column('i')->lessThan(5);

        static::assertSame(Truth::Always, ZoneTruth::of($lt, ['i' => ZoneMapMother::clean(1, 4)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($lt, ['i' => ZoneMapMother::withNulls(1, 4)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($lt, ['i' => ZoneMapMother::unknownNulls(1, 4)]));
    }

    public function test_comparison_negated_never_when_all_null_or_every_value_matches(): void
    {
        $notLt = parquet_not(parquet_column('i')->lessThan(5));

        static::assertSame(Truth::Never, ZoneTruth::of($notLt, ['i' => ZoneMapMother::allNull()]));
        static::assertSame(Truth::Never, ZoneTruth::of($notLt, ['i' => ZoneMapMother::withNulls(1, 4)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($notLt, ['i' => ZoneMapMother::withNulls(1, 5)]));
    }

    public function test_comparison_negated_always_when_no_null_and_the_bounds_miss_the_interval(): void
    {
        $notLt = parquet_not(parquet_column('i')->lessThan(5));

        static::assertSame(Truth::Always, ZoneTruth::of($notLt, ['i' => ZoneMapMother::clean(5, 9)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($notLt, ['i' => ZoneMapMother::withNulls(5, 9)]));
    }

    public function test_in_is_the_union_of_its_equalities(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->in([1, 9]), ['i' => ZoneMapMother::clean(
            3,
            7,
        )]));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('i')->in([3]), ['i' => ZoneMapMother::clean(
            3,
            3,
        )]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->in([3, 9]), ['i' => ZoneMapMother::clean(
            3,
            7,
        )]));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->notIn([3]), ['i' => ZoneMapMother::clean(
            3,
            3,
        )]));
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_column('i')->notIn([1, 9]),
            ['i' => ZoneMapMother::clean(3, 7)],
        ));
    }

    public function test_an_empty_in_never_matches_and_its_negation_follows_the_nulls(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->in([]), ['i' => ZoneMapMother::clean(
            1,
            4,
        )]));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->in([]), []));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('i')->notIn([]), ['i' => ZoneMapMother::clean(
            1,
            4,
        )]));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->notIn([]), [
            'i' => ZoneMapMother::allNull(),
        ]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->notIn([]), ['i' => ZoneMapMother::withNulls(
            1,
            4,
        )]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->notIn([]), []));
    }

    public function test_is_null_positive_never_without_nulls_and_always_when_all_null(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->isNull(), ['i' => ZoneMapMother::clean(
            1,
            4,
        )]));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('i')->isNull(), [
            'i' => ZoneMapMother::allNull(),
        ]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->isNull(), ['i' => ZoneMapMother::withNulls(
            1,
            4,
        )]));
    }

    public function test_is_null_negated_never_when_all_null_and_always_without_nulls(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->isNotNull(), [
            'i' => ZoneMapMother::allNull(),
        ]));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('i')->isNotNull(), ['i' => ZoneMapMother::clean(
            1,
            4,
        )]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('i')->isNotNull(),
            ['i' => ZoneMapMother::withNulls(1, 4)],
        ));
    }

    public function test_an_unknown_null_count_gives_maybe_for_is_null(): void
    {
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('i')->isNull(),
            ['i' => ZoneMapMother::unknownNulls(1, 4)],
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('i')->isNotNull(),
            ['i' => ZoneMapMother::unknownNulls(1, 4)],
        ));
    }

    public function test_is_null_needs_no_trusted_bound(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('i')->isNull(), [
            'i' => ZoneMapMother::untrusted(),
        ]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->lessThan(5), [
            'i' => ZoneMapMother::untrusted(),
        ]));
    }

    public function test_starts_with_positive_never_when_all_null_or_the_bounds_miss_the_prefix(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('s')->startsWith('b'), [
            's' => ZoneMapMother::allNull(),
        ]));
        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_column('s')->startsWith('b'),
            ['s' => ZoneMapMother::withNulls('c', 'd')],
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('s')->startsWith('b'),
            ['s' => ZoneMapMother::withNulls('a', 'c')],
        ));
    }

    public function test_starts_with_positive_always_when_both_bounds_start_with_the_prefix(): void
    {
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_column('s')->startsWith('ab'),
            ['s' => ZoneMapMother::clean('ab', 'abz')],
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('s')->startsWith('ab'),
            ['s' => ZoneMapMother::withNulls('ab', 'abz')],
        ));
    }

    public function test_starts_with_negated_never_when_all_null_or_both_bounds_start_with_the_prefix(): void
    {
        $notPrefix = parquet_not(parquet_column('s')->startsWith('ab'));

        static::assertSame(Truth::Never, ZoneTruth::of($notPrefix, ['s' => ZoneMapMother::allNull()]));
        static::assertSame(Truth::Never, ZoneTruth::of($notPrefix, ['s' => ZoneMapMother::withNulls('ab', 'abz')]));
    }

    public function test_starts_with_negated_always_when_no_null_and_the_bounds_miss_the_prefix(): void
    {
        $notPrefix = parquet_not(parquet_column('s')->startsWith('ab'));

        static::assertSame(Truth::Always, ZoneTruth::of($notPrefix, ['s' => ZoneMapMother::clean('b', 'c')]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($notPrefix, ['s' => ZoneMapMother::withNulls('b', 'c')]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($notPrefix, ['s' => ZoneMapMother::untrusted()]));
    }

    public function test_an_empty_prefix_matches_every_value_that_is_not_null(): void
    {
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_column('s')->startsWith(''),
            ['s' => ZoneMapMother::clean('', "\xFF")],
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('s')->startsWith(''),
            ['s' => ZoneMapMother::withNulls('a', 'b')],
        ));
    }

    public function test_an_all_0xff_prefix_has_no_upper_bound(): void
    {
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_column('s')->startsWith("\xFF"),
            ['s' => ZoneMapMother::clean("\xFF", "\xFF\xFF")],
        ));
        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_column('s')->startsWith("\xFF"),
            ['s' => ZoneMapMother::clean('a', "\xFE")],
        ));
    }

    public function test_all_is_never_with_any_never_and_always_with_all_always(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4), 's' => ZoneMapMother::withNulls('a', 'b')];

        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_all(parquet_column('i')->lessThan(5), parquet_column('i')->greaterThan(9)),
            $zones,
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_all(parquet_column('i')->lessThan(5), parquet_column('i')->greaterThan(0)),
            $zones,
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_all(parquet_column('i')->lessThan(5), parquet_column('s')->equals('a')),
            $zones,
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_all(), $zones));
    }

    public function test_any_is_never_with_all_never_and_always_with_any_always(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4), 's' => ZoneMapMother::withNulls('a', 'b')];

        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_any(parquet_column('i')->greaterThan(5), parquet_column('i')->lessThan(0)),
            $zones,
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_any(parquet_column('s')->equals('a'), parquet_column('i')->lessThan(5)),
            $zones,
        ));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_any(parquet_column('i')->greaterThan(5), parquet_column('s')->equals('a')),
            $zones,
        ));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_any(), $zones));
    }

    public function test_a_negated_all_is_the_any_of_its_negated_children(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4)];

        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_not(parquet_all(parquet_column('i')->lessThan(5), parquet_column('i')->greaterThan(9))),
            $zones,
        ));
        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_not(parquet_all(parquet_column('i')->lessThan(5), parquet_column('i')->greaterThan(0))),
            $zones,
        ));
    }

    public function test_a_negated_any_is_the_all_of_its_negated_children(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4)];

        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_not(parquet_any(parquet_column('i')->greaterThan(5), parquet_column('i')->lessThan(0))),
            $zones,
        ));
        static::assertSame(Truth::Never, ZoneTruth::of(
            parquet_not(parquet_any(parquet_column('i')->greaterThan(5), parquet_column('i')->lessThan(5))),
            $zones,
        ));
    }

    public function test_a_double_negation_is_the_child(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4)];

        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_not(parquet_not(parquet_column('i')->lessThan(5))),
            $zones,
        ));
    }

    public function test_an_all_nan_zone_never_matches_and_its_negation_follows_the_nulls(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('f')->lessThan(5.0), [
            'f' => ZoneMapMother::allNaN(),
        ]));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_not(parquet_column('f')->lessThan(5.0)), [
            'f' => ZoneMapMother::allNaN(),
        ]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_not(parquet_column('f')->lessThan(5.0)), [
            'f' => ZoneMapMother::allNaN(2),
        ]));
    }

    public function test_an_infinity_bound_is_a_value_of_the_zone(): void
    {
        $toInfinity = ['f' => ZoneMapMother::ieee(1.0, INF, nanCount: 0)];

        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('f')->equals(INF), $toInfinity));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('f')->greaterThanOrEqual(INF), $toInfinity));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_not(parquet_column('f')->equals(INF)), $toInfinity));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('f')->greaterThan(-INF), [
            'f' => ZoneMapMother::ieee(-INF, 1.0, nanCount: 0),
        ]));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_not(parquet_column('f')->equals(INF)), [
            'f' => ZoneMapMother::ieee(INF, INF, nanCount: 0),
        ]));
    }

    public function test_not_less_than_needs_no_nan_to_be_never(): void
    {
        $notLt = parquet_not(parquet_column('f')->lessThan(5.0));

        static::assertSame(Truth::Maybe, ZoneTruth::of($notLt, ['f' => ZoneMapMother::unknownNaN(1.0, 4.0)]));
        static::assertSame(Truth::Maybe, ZoneTruth::of($notLt, ['f' => ZoneMapMother::ieee(1.0, 4.0, nanCount: 1)]));
        static::assertSame(Truth::Never, ZoneTruth::of($notLt, ['f' => ZoneMapMother::ieee(1.0, 4.0, nanCount: 0)]));
    }

    public function test_not_equals_needs_no_nan_to_be_never(): void
    {
        $notEquals = parquet_column('f')->notEquals(3.0);

        static::assertSame(Truth::Maybe, ZoneTruth::of($notEquals, ['f' => ZoneMapMother::unknownNaN(3.0, 3.0)]));
        static::assertSame(Truth::Never, ZoneTruth::of($notEquals, ['f' => ZoneMapMother::ieee(
            3.0,
            3.0,
            nanCount: 0,
        )]));
    }

    public function test_not_in_needs_no_nan_to_be_never(): void
    {
        $notIn = parquet_column('f')->notIn([3.0, 4.0]);

        static::assertSame(Truth::Maybe, ZoneTruth::of($notIn, ['f' => ZoneMapMother::unknownNaN(3.0, 3.0)]));
        static::assertSame(Truth::Never, ZoneTruth::of($notIn, ['f' => ZoneMapMother::ieee(3.0, 3.0, nanCount: 0)]));
    }

    public function test_a_float_comparison_is_always_only_without_nan(): void
    {
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('f')->lessThan(5.0),
            ['f' => ZoneMapMother::unknownNaN(1.0, 4.0)],
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('f')->lessThan(5.0), ['f' => ZoneMapMother::ieee(
            1.0,
            4.0,
            nanCount: 0,
        )]));
    }

    public function test_inexact_bounds_never_prove_an_equality(): void
    {
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('s')->equals('ab'),
            ['s' => ZoneMapMother::inexact('ab', 'ab')],
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('s')->equals('ab'), ['s' => ZoneMapMother::clean(
            'ab',
            'ab',
        )]));
        static::assertSame(Truth::Maybe, ZoneTruth::of(
            parquet_column('s')->notEquals('ab'),
            ['s' => ZoneMapMother::inexact('ab', 'ab')],
        ));
        static::assertSame(Truth::Always, ZoneTruth::of(
            parquet_column('s')->lessThan('b'),
            ['s' => ZoneMapMother::inexact('aa', 'ab')],
        ));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('s')->equals('b'), ['s' => ZoneMapMother::inexact(
            'aa',
            'ab',
        )]));
    }

    public function test_an_unbound_leaf_is_maybe_in_both_polarities(): void
    {
        $zones = ['i' => ZoneMapMother::clean(1, 4)];

        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->equals('1'), $zones));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->notEquals('1'), $zones));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_not(parquet_column('i')->startsWith('1')), $zones));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->startsWith('1'), $zones));
    }

    public function test_an_absent_column_is_an_all_null_zone(): void
    {
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('missing')->equals(1), []));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('missing')->notEquals(1), []));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('missing')->startsWith('a'), []));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_not(parquet_column('missing')->startsWith('a')), []));
        static::assertSame(Truth::Always, ZoneTruth::of(parquet_column('missing')->isNull(), []));
        static::assertSame(Truth::Never, ZoneTruth::of(parquet_column('missing')->isNotNull(), []));
    }

    public function test_a_column_without_usable_statistics_is_maybe(): void
    {
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->lessThan(5), []));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('i')->isNull(), []));
        static::assertSame(Truth::Maybe, ZoneTruth::of(parquet_column('s')->startsWith('a'), []));
    }
}
