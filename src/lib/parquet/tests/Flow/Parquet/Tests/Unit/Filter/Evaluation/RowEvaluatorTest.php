<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use DateTimeImmutable;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\RowEvaluator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use TypeError;

use function Flow\Parquet\DSL\parquet_column;

use const NAN;

final class RowEvaluatorTest extends TestCase
{
    /**
     * @param 'lt'|'not lt'|'not not lt'|'any lt null'|'not all lt not null'|'not any lt null' $filter
     */
    #[TestWith(['lt', null, false])]
    #[TestWith(['lt', 3, true])]
    #[TestWith(['lt', 7, false])]
    #[TestWith(['not lt', null, false])]
    #[TestWith(['not lt', 3, false])]
    #[TestWith(['not lt', 7, true])]
    #[TestWith(['not not lt', null, false])]
    #[TestWith(['not not lt', 3, true])]
    #[TestWith(['any lt null', null, true])]
    #[TestWith(['any lt null', 3, true])]
    #[TestWith(['any lt null', 7, false])]
    #[TestWith(['not all lt not null', null, true])]
    #[TestWith(['not all lt not null', 3, false])]
    #[TestWith(['not all lt not null', 7, true])]
    #[TestWith(['not any lt null', null, false])]
    #[TestWith(['not any lt null', 3, false])]
    #[TestWith(['not any lt null', 7, true])]
    public function test_a_null_is_true_under_neither_polarity(string $filter, ?int $value, bool $kept): void
    {
        $lt = parquet_column('i')->lessThan(5);
        $null = parquet_column('i')->isNull();

        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'lt' => $lt,
                        'not lt' => $lt->negate(),
                        'not not lt' => $lt->negate()->negate(),
                        'any lt null' => $lt->or($null),
                        'not all lt not null' => $lt->and($null->negate())->negate(),
                        'not any lt null' => $lt->or($null)->negate(),
                    },
                    Schema::with(FlatColumn::int64('i')),
                    new Options(),
                ),
                ['i' => [$value]],
                1,
            ),
        );
    }

    /**
     * @param 'lt'|'not lt'|'not equals'|'not in' $filter
     */
    #[TestWith(['lt', NAN, false])]
    #[TestWith(['lt', 3.0, true])]
    #[TestWith(['lt', 5.0, false])]
    #[TestWith(['lt', null, false])]
    #[TestWith(['not lt', NAN, true])]
    #[TestWith(['not lt', 3.0, false])]
    #[TestWith(['not lt', 5.0, true])]
    #[TestWith(['not lt', null, false])]
    #[TestWith(['not equals', NAN, true])]
    #[TestWith(['not equals', 5.0, false])]
    #[TestWith(['not equals', 1.0, true])]
    #[TestWith(['not equals', null, false])]
    #[TestWith(['not in', NAN, true])]
    #[TestWith(['not in', 1.0, false])]
    #[TestWith(['not in', 3.0, true])]
    #[TestWith(['not in', null, false])]
    public function test_nan_compares_false_so_every_negation_keeps_it(string $filter, ?float $value, bool $kept): void
    {
        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'lt' => parquet_column('f')->lessThan(5.0),
                        'not lt' => parquet_column('f')->lessThan(5.0)->negate(),
                        'not equals' => parquet_column('f')->notEquals(5.0),
                        'not in' => parquet_column('f')->notIn([1.0, 5.0]),
                    },
                    Schema::with(FlatColumn::double('f')),
                    new Options(),
                ),
                ['f' => [$value]],
                1,
            ),
        );
    }

    /**
     * @param 'f equals 0.0'|'f equals -0.0'|'f lt 0.0'|'f gte 0.0'|'r in -0.0'|'r gt -0.0' $filter
     */
    #[TestWith(['f equals 0.0', -0.0, true])]
    #[TestWith(['f equals 0.0', 0.0, true])]
    #[TestWith(['f equals -0.0', -0.0, true])]
    #[TestWith(['f equals -0.0', 0.0, true])]
    #[TestWith(['f lt 0.0', -0.0, false])]
    #[TestWith(['f gte 0.0', -0.0, true])]
    #[TestWith(['r in -0.0', 0.0, true])]
    #[TestWith(['r gt -0.0', 0.0, false])]
    public function test_negative_zero_equals_positive_zero(string $filter, float $value, bool $kept): void
    {
        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'f equals 0.0' => parquet_column('f')->equals(0.0),
                        'f equals -0.0' => parquet_column('f')->equals(-0.0),
                        'f lt 0.0' => parquet_column('f')->lessThan(0.0),
                        'f gte 0.0' => parquet_column('f')->greaterThanOrEqual(0.0),
                        'r in -0.0' => parquet_column('r')->in([-0.0]),
                        'r gt -0.0' => parquet_column('r')->greaterThan(-0.0),
                    },
                    Schema::with(FlatColumn::double('f'), FlatColumn::float('r')),
                    new Options(),
                ),
                ['f' => [$value], 'r' => [$value]],
                1,
            ),
        );
    }

    /**
     * @param 'equals'|'lt'|'gt zero' $filter
     */
    #[TestWith(['equals', "\x04\xD2", true])]
    #[TestWith(['equals', "\x04\xD1", false])]
    #[TestWith(['lt', "\x04\xD2", false])]
    #[TestWith(['lt', "\x04\xD1", true])]
    #[TestWith(['lt', "\xFB\x2E", true])]
    #[TestWith(['gt zero', "\x04\xD1", true])]
    #[TestWith(['gt zero', "\xFB\x2E", false])]
    public function test_decimal_unscaled_bytes_compare_as_signed_numbers(
        string $filter,
        string $value,
        bool $kept,
    ): void {
        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'equals' => parquet_column('d')->equals(12.34),
                        'lt' => parquet_column('d')->lessThan(12.34),
                        'gt zero' => parquet_column('d')->greaterThan(0),
                    },
                    Schema::with(FlatColumnMother::flbaDecimal()),
                    new Options(),
                ),
                ['d' => [$value]],
                1,
            ),
        );
    }

    /**
     * @param 'equals'|'at most' $filter
     */
    #[TestWith(['equals', 999, false])]
    #[TestWith(['equals', 1000, true])]
    #[TestWith(['equals', 1999, true])]
    #[TestWith(['equals', 2000, false])]
    #[TestWith(['at most', 1999, true])]
    #[TestWith(['at most', 2000, false])]
    public function test_nanoseconds_match_the_microsecond_they_decode_to(string $filter, int $nanos, bool $kept): void
    {
        $microsecond = new DateTimeImmutable('1970-01-01 00:00:00.000001 UTC');

        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'equals' => parquet_column('ts')->equals($microsecond),
                        'at most' => parquet_column('ts')->lessThanOrEqual($microsecond),
                    },
                    Schema::with(FlatColumnMother::timestamp(TimeUnit::NANOSECONDS)),
                    new Options(),
                ),
                ['ts' => [$nanos]],
                1,
            ),
        );
    }

    #[TestWith([0, false])]
    #[TestWith([1, true])]
    #[TestWith([3, false])]
    #[TestWith([5, true])]
    #[TestWith([7, false])]
    #[TestWith([9, true])]
    #[TestWith([10, false])]
    public function test_a_value_is_found_among_the_sorted_values_of_a_set(int $value, bool $kept): void
    {
        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    parquet_column('i')->in([9, 1, 5]),
                    Schema::with(FlatColumn::int64('i')),
                    new Options(),
                ),
                ['i' => [$value]],
                1,
            ),
        );
    }

    #[TestWith([-1, true])]
    #[TestWith([5, false])]
    public function test_an_unsigned_64_bit_pattern_compares_unsigned(int $bits, bool $greaterThanTen): void
    {
        static::assertSame(
            [$greaterThanTen],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    parquet_column('u')->greaterThan(10),
                    Schema::with(FlatColumnMother::uint64()),
                    new Options(),
                ),
                ['u' => [$bits]],
                1,
            ),
        );
    }

    /**
     * @param 'is null'|'not is null'|'equals'|'not equals'|'not starts with' $filter
     */
    #[TestWith(['is null', true])]
    #[TestWith(['not is null', false])]
    #[TestWith(['equals', false])]
    #[TestWith(['not equals', false])]
    #[TestWith(['not starts with', false])]
    public function test_an_absent_column_is_null_in_every_row(string $filter, bool $kept): void
    {
        static::assertSame(
            [$kept, $kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    match ($filter) {
                        'is null' => parquet_column('missing')->isNull(),
                        'not is null' => parquet_column('missing')->isNull()->negate(),
                        'equals' => parquet_column('missing')->equals(1),
                        'not equals' => parquet_column('missing')->equals(1)->negate(),
                        'not starts with' => parquet_column('missing')->startsWith('a')->negate(),
                    },
                    Schema::with(FlatColumn::int64('i')),
                    new Options(),
                ),
                [],
                2,
            ),
        );
    }

    #[TestWith(['', true])]
    #[TestWith(['abc', true])]
    #[TestWith([null, false])]
    public function test_an_empty_prefix_starts_every_non_null_value(?string $value, bool $kept): void
    {
        $schema = Schema::with(FlatColumn::string('s'));

        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(parquet_column('s')->startsWith(''), $schema, new Options()),
                ['s' => [$value]],
                1,
            ),
        );
        static::assertSame(
            [false],
            RowEvaluator::evaluate(
                BoundFilter::bind(parquet_column('s')->startsWith('')->negate(), $schema, new Options()),
                ['s' => [$value]],
                1,
            ),
        );
    }

    #[TestWith(["ab\x00", true])]
    #[TestWith(['a', false])]
    #[TestWith(['ba', false])]
    public function test_a_prefix_compares_raw_bytes(string $value, bool $kept): void
    {
        static::assertSame(
            [$kept],
            RowEvaluator::evaluate(
                BoundFilter::bind(
                    parquet_column('s')->startsWith('ab'),
                    Schema::with(FlatColumn::string('s')),
                    new Options(),
                ),
                ['s' => [$value]],
                1,
            ),
        );
    }

    public function test_a_leaf_not_answered_exactly_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows are evaluated on leaves answered exactly, the leaf on "i" is not');

        RowEvaluator::evaluate(
            BoundFilter::bind(parquet_column('i')->equals('1'), Schema::with(FlatColumn::int64('i')), new Options()),
            ['i' => [1]],
            1,
        );
    }

    public function test_is_null_is_true_on_a_null_row_and_is_not_null_on_a_value(): void
    {
        $schema = Schema::with(FlatColumn::int64('i'));

        static::assertSame(
            [true, false, true],
            RowEvaluator::evaluate(
                BoundFilter::bind(parquet_column('i')->isNull(), $schema, new Options()),
                ['i' => [null, 3, null]],
                3,
            ),
        );
        static::assertSame(
            [false, true, false],
            RowEvaluator::evaluate(
                BoundFilter::bind(parquet_column('i')->isNotNull(), $schema, new Options()),
                ['i' => [null, 3, null]],
                3,
            ),
        );
    }

    public function test_a_bound_column_without_values_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No physical values of column "i"');

        RowEvaluator::evaluate(
            BoundFilter::bind(parquet_column('i')->isNull(), Schema::with(FlatColumn::int64('i')), new Options()),
            [],
            1,
        );
    }

    public function test_values_for_another_row_count_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "i" holds 1 physical values for 2 rows');

        RowEvaluator::evaluate(
            BoundFilter::bind(parquet_column('i')->isNull(), Schema::with(FlatColumn::int64('i')), new Options()),
            ['i' => [1]],
            2,
        );
    }

    public function test_a_value_that_is_no_physical_value_is_refused(): void
    {
        $this->expectException(TypeError::class);

        RowEvaluator::evaluate(
            BoundFilter::bind(parquet_column('i')->equals(1), Schema::with(FlatColumn::int64('i')), new Options()),
            ['i' => [[1]]],
            1,
        );
    }
}
