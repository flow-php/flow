<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use DateTimeImmutable;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

use const INF;
use const NAN;

final class FilterEvaluatorTest extends TestCase
{
    public function test_a_comparison_on_null_is_null_and_so_is_its_negation(): void
    {
        $schema = Schema::with(FlatColumn::int64('i'));
        $batch = ['i' => [1, null, 2]];

        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('i')->equals(1), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [2],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('i')->notEquals(1), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('i')->isNull(), $schema, new Options())))->mask(
                $batch,
            ),
        );
    }

    public function test_nan_equals_nothing_and_its_negation_holds(): void
    {
        $schema = Schema::with(FlatColumn::double('f'));
        $batch = ['f' => [NAN, 1.0, -0.0, 7.0]];

        static::assertSame(
            [1, 2],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('f')->lessThan(5.0), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('f')->in([1.0, 9.0]), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [0, 3],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('f')->lessThan(5.0)->negate(),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [2],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('f')->equals(0.0), $schema, new Options())))->mask(
                $batch,
            ),
        );
    }

    public function test_strings_compare_in_byte_order_and_a_non_ascii_prefix_matches_bytes(): void
    {
        $schema = Schema::with(FlatColumn::string('s'));
        $batch = ['s' => ['10', '9', 'a', 'źdźbło', 'zebra']];

        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('s')->lessThan('9'), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [3],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('s')->startsWith('ź'),
                $schema,
                new Options(),
            )))->mask($batch),
        );
    }

    public function test_unsigned_64_bit_values_compare_numerically_with_ints_digit_strings_and_floats(): void
    {
        $schema = Schema::with(FlatColumnMother::uint64());
        $batch = ['u' => ['2', '10', '18446744073709551615']];

        static::assertSame(
            [1, 2],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('u')->greaterThan(9), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('u')->equals('10'), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('u')->lessThan(2.5), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('u')->greaterThan(1.0e19),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('u')->greaterThan(INF),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [0, 1, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('u')->greaterThan(-1),
                $schema,
                new Options(),
            )))->mask($batch),
        );
    }

    public function test_a_float_on_an_integer_column_compares_numerically(): void
    {
        $schema = Schema::with(FlatColumn::int32('i'));

        static::assertSame(
            [1, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('i')->greaterThan(2.5),
                $schema,
                new Options(),
            )))->mask(['i' => [
                2,
                3,
                4,
            ]]),
        );
    }

    public function test_uuid_boolean_decimal_and_float32_columns_compare_their_decoded_values(): void
    {
        $schema = Schema::with(
            FlatColumn::uuid('uid'),
            FlatColumn::boolean('flag'),
            FlatColumn::decimal('amount', 10, 2),
            FlatColumn::float('ratio'),
        );
        $batch = [
            'uid' => ['00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002'],
            'flag' => [true, false],
            'amount' => [12.25, 3.5],
            'ratio' => [20.5, 0.10000000149011612],
        ];

        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('uid')->greaterThan('00000000-0000-4000-8000-000000000001'),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('flag')->equals(false),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('amount')->equals(12.25),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('ratio')->equals(0.1),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('ratio')->equals(20.5),
                $schema,
                new Options(),
            )))->mask($batch),
        );
    }

    public function test_an_uppercase_uuid_literal_matches_the_lowercase_value_it_names(): void
    {
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->in([
                    '00000000-0000-4000-8000-0000000000AA',
                    '00000000-0000-4000-8000-0000000000BB',
                ]),
                Schema::with(FlatColumn::uuid('id')),
                new Options(),
            )))->mask(['id' => ['00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-0000000000aa']]),
        );
    }

    public function test_a_mixed_in_matches_each_kind_numerically(): void
    {
        static::assertSame(
            [0, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('i')->in([3, 33.0]),
                Schema::with(FlatColumn::int64('i')),
                new Options(),
            )))->mask(['i' => [3, 4, 33]]),
        );
    }

    public function test_an_in_orders_a_value_beyond_2_pow_53_against_literals_it_hashes(): void
    {
        static::assertSame(
            [0, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('i')->in([(float) 2 ** 53, 5]),
                Schema::with(FlatColumn::int64('i')),
                new Options(),
            )))->mask(['i' => [2 ** 53 + 1, 2 ** 53 + 3, 5]]),
        );
    }

    public function test_an_in_matches_decoded_numbers_date_times_and_unsigned_64_bit_values_by_key(): void
    {
        static::assertSame(
            [0, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('d')->in([1, 2.5]),
                Schema::with(FlatColumn::double('d')),
                new Options(),
            )))->mask(['d' => [1.0, 2.0, 2.5, NAN]]),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('t')->in([new DateTimeImmutable('2024-01-01 02:00 +02:00')]),
                Schema::with(FlatColumn::dateTime('t')),
                new Options(),
            )))->mask(['t' => [new DateTimeImmutable('2024-01-02 UTC'), new DateTimeImmutable('2024-01-01 UTC')]]),
        );
        static::assertSame(
            [0, 2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('u')->in([2, '18446744073709551615']),
                Schema::with(FlatColumnMother::uint64()),
                new Options(),
            )))->mask(['u' => ['2', '10', '18446744073709551615']]),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('u')->in([10.0]),
                Schema::with(FlatColumnMother::uint64()),
                new Options(),
            )))->mask(['u' => ['2', '10']]),
        );
    }

    public function test_date_times_compare_by_instant(): void
    {
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('t')->equals(new DateTimeImmutable('2024-01-01 02:00 +02:00')),
                Schema::with(FlatColumn::dateTime('t')),
                new Options(),
            )))->mask([
                't' => [
                    new DateTimeImmutable('2024-01-01 00:00 UTC'),
                    new DateTimeImmutable('2024-01-01 00:00:01 UTC'),
                ],
            ]),
        );
    }

    public function test_a_json_column_compares_its_text(): void
    {
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('j')->startsWith('{"a"'),
                Schema::with(FlatColumn::json('j')),
                new Options(),
            )))->mask(['j' => ['{"a": 1}', '[1]']]),
        );
    }

    public function test_a_struct_leaf_is_read_through_its_struct_and_a_null_struct_holds_a_null_leaf(): void
    {
        $schema = Schema::with(NestedColumn::struct('address', [FlatColumn::string('city')]));
        $batch = ['address' => [['city' => 'x'], null, ['city' => 'y']]];

        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('address.city')->equals('x'),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('address.city')->isNull(),
                $schema,
                new Options(),
            )))->mask($batch),
        );
    }

    public function test_a_column_the_file_lacks_is_null(): void
    {
        $schema = Schema::with(FlatColumn::int64('i'));

        static::assertSame(
            [0, 1],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('missing')->isNull(),
                $schema,
                new Options(),
            )))->mask(['i' => [1, 2]]),
        );
        static::assertSame(
            [],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('missing')->equals(1),
                $schema,
                new Options(),
            )))->mask(['i' => [1, 2]]),
        );
        static::assertSame(
            [],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('missing')->notEquals(1),
                $schema,
                new Options(),
            )))->mask(['i' => [
                1,
                2,
            ]]),
        );
    }

    public function test_composites_follow_three_valued_logic(): void
    {
        $schema = Schema::with(FlatColumn::int64('i'), FlatColumn::int64('n'));
        $batch = ['i' => [1, 1, 2], 'n' => [null, 1, null]];
        $unknown = parquet_column('n')->equals(1);

        static::assertSame(
            [0, 1],
            (new FilterEvaluator(BoundFilter::bind(
                $unknown->or(parquet_column('i')->equals(1)),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [1],
            (new FilterEvaluator(BoundFilter::bind(
                $unknown->and(parquet_column('i')->equals(1)),
                $schema,
                new Options(),
            )))->mask($batch),
        );
        static::assertSame(
            [0],
            (new FilterEvaluator(BoundFilter::bind(
                $unknown->and(parquet_column('i')->equals(2))->negate(),
                $schema,
                new Options(),
            )))->mask(['i' => [1, 2], 'n' => [null, null]]),
        );
    }

    public function test_in_and_starts_with_on_null_are_null(): void
    {
        $schema = Schema::with(FlatColumn::string('s'));
        $batch = ['s' => [null, 'a', 'b']];

        static::assertSame(
            [2],
            (new FilterEvaluator(BoundFilter::bind(parquet_column('s')->notIn(['a']), $schema, new Options())))->mask(
                $batch,
            ),
        );
        static::assertSame(
            [2],
            (new FilterEvaluator(BoundFilter::bind(
                parquet_column('s')->startsWith('a')->negate(),
                $schema,
                new Options(),
            )))->mask($batch),
        );
    }

    public function test_the_roots_are_the_root_columns_of_the_present_leaves(): void
    {
        static::assertSame(
            ['address', 'i'],
            (new FilterEvaluator(BoundFilter::bind(
                new All(
                    parquet_column('address.city')->isNull(),
                    parquet_column('i')->equals(1),
                    parquet_column('missing')->isNull(),
                ),
                Schema::with(FlatColumn::int64('i'), NestedColumn::struct('address', [FlatColumn::string('city')])),
                new Options(),
            )))->roots(),
        );
    }

    #[TestWith(['u', ' 9', 'Column "u" (INT64 (INTEGER)) cannot hold the string literal \' 9\''])]
    #[TestWith(['u', '1e1', 'Column "u" (INT64 (INTEGER)) cannot hold the string literal \'1e1\''])]
    #[TestWith(['u', '09', 'Column "u" (INT64 (INTEGER)) cannot hold the string literal \'09\''])]
    #[TestWith(['i', '1', 'Column "i" (INT64) cannot hold the string literal \'1\''])]
    #[TestWith(['b', 1, 'Column "b" (BOOLEAN) cannot hold the int literal 1'])]
    #[TestWith([
        't',
        '2024-01-01',
        'Column "t" (INT64 (TIMESTAMP)) cannot hold the string literal \'2024-01-01\'',
    ])]
    public function test_a_literal_the_column_cannot_hold_is_refused(
        string $column,
        int|string $literal,
        string $message,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        new FilterEvaluator(BoundFilter::bind(
            parquet_column($column)->equals($literal),
            Schema::with(
                FlatColumnMother::uint64(),
                FlatColumn::int64('i'),
                FlatColumn::boolean('b'),
                FlatColumn::dateTime('t'),
                FlatColumn::uuid('id'),
            ),
            new Options(),
        ));
    }

    public function test_a_prefix_on_a_column_without_strings_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "i" (INT64) holds no strings for a prefix');

        new FilterEvaluator(BoundFilter::bind(
            parquet_column('i')->startsWith('1'),
            Schema::with(FlatColumn::int64('i')),
            new Options(),
        ));
    }

    public function test_a_leaf_inside_a_list_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filter column "tags.list.element" lies inside a list or a map, where a row holds no single value',
        );

        new FilterEvaluator(BoundFilter::bind(
            parquet_column('tags.list.element')->equals('a'),
            Schema::with(NestedColumn::list('tags', ListElement::string())),
            new Options(),
        ));
    }

    public function test_a_struct_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filter column "address" is not a leaf column');

        new FilterEvaluator(BoundFilter::bind(
            parquet_column('address')->isNull(),
            Schema::with(NestedColumn::struct('address', [FlatColumn::string('city')])),
            new Options(),
        ));
    }
}
