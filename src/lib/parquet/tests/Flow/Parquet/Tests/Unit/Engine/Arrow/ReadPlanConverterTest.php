<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use DateTimeImmutable;
use Flow\Parquet\Engine\Arrow\ReadPlanConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

final class ReadPlanConverterTest extends TestCase
{
    /**
     * @return iterable<string, array{Filter, array<string, mixed>}>
     */
    public static function filters(): iterable
    {
        yield 'equal' => [
            parquet_column('id')->equals(3),
            [
                'leaf' => 'intervals',
                'path' => ['id'],
                'physical' => 'INT64',
                'intervals' => [[3, true, 3, true]],
            ],
        ];
        yield 'less than, unbounded below' => [
            parquet_column('id')->lessThan(3),
            [
                'leaf' => 'intervals',
                'path' => ['id'],
                'physical' => 'INT64',
                'intervals' => [[null, false, 3, false]],
            ],
        ];
        yield 'in, one interval per value' => [
            parquet_column('id')->in([1, 5]),
            [
                'leaf' => 'intervals',
                'path' => ['id'],
                'physical' => 'INT64',
                'intervals' => [[1, true, 1, true], [5, true, 5, true]],
            ],
        ];
        yield 'empty in' => [
            parquet_column('id')->in([]),
            [
                'leaf' => 'intervals',
                'path' => ['id'],
                'physical' => 'INT64',
                'intervals' => [],
            ],
        ];
        yield 'double, negative zero as zero' => [
            parquet_column('score')->greaterThanOrEqual(-0.0),
            [
                'leaf' => 'intervals',
                'path' => ['score'],
                'physical' => 'DOUBLE',
                'intervals' => [[0.0, true, null, false]],
            ],
        ];
        yield 'decimal as unscaled big-endian bytes' => [
            parquet_column('d')->equals(12.34),
            [
                'leaf' => 'intervals',
                'path' => ['d'],
                'physical' => 'FIXED_LEN_BYTE_ARRAY',
                'intervals' => [["\x00\x00\x00\x00\x00\x00\x04\xD2", true, "\x00\x00\x00\x00\x00\x00\x04\xD2", true]],
            ],
        ];
        yield 'date as days' => [
            parquet_column('day')->equals(new DateTimeImmutable('1970-01-03 00:00:00 UTC')),
            [
                'leaf' => 'intervals',
                'path' => ['day'],
                'physical' => 'INT32',
                'intervals' => [[2, true, 2, true]],
            ],
        ];
        yield 'struct leaf path' => [
            parquet_column('address.city')->equals('a'),
            [
                'leaf' => 'intervals',
                'path' => ['address', 'city'],
                'physical' => 'BYTE_ARRAY',
                'intervals' => [['a', true, 'a', true]],
            ],
        ];
        yield 'is null' => [parquet_column('id')->isNull(), ['leaf' => 'is_null', 'path' => ['id']]];
        yield 'starts with' => [
            parquet_column('address.city')->startsWith('ab'),
            [
                'leaf' => 'starts_with',
                'path' => ['address', 'city'],
                'prefix' => 'ab',
            ],
        ];
        yield 'absent' => [parquet_column('missing')->equals(1), ['leaf' => 'absent', 'is_null' => false]];
        yield 'is null on an absent column' => [
            parquet_column('missing')->isNull(),
            ['leaf' => 'absent', 'is_null' => true],
        ];
        yield 'nested not and or' => [
            parquet_all(
                parquet_column('id')->isNull(),
                parquet_not(parquet_any(parquet_column('id')->isNull(), parquet_column('missing')->startsWith('a'))),
            ),
            [
                'and' => [
                    ['leaf' => 'is_null', 'path' => ['id']],
                    [
                        'not' => [
                            'or' => [['leaf' => 'is_null', 'path' => ['id']], ['leaf' => 'absent', 'is_null' => false]],
                        ],
                    ],
                ],
            ],
        ];
    }

    #[DataProvider('filters')]
    public function test_the_row_filter_crosses_as_a_tree_of_physical_leaves(Filter $filter, array $expected): void
    {
        static::assertSame(
            ['selectors' => [[4, false]], 'filter' => $expected, 'limit' => null],
            ReadPlanConverter::toExtension(ReadPlanMother::rowFiltered(
                4,
                $filter,
                Schema::with(
                    FlatColumn::int64('id'),
                    FlatColumn::double('score'),
                    FlatColumnMother::flbaDecimal(),
                    FlatColumn::date('day'),
                    NestedColumn::struct('address', [FlatColumn::string('city')]),
                ),
            )),
        );
    }

    public function test_only_the_bound_conjuncts_cross(): void
    {
        static::assertSame(
            ['leaf' => 'is_null', 'path' => ['id']],
            ReadPlanConverter::toExtension(ReadPlanMother::rowFiltered(
                4,
                parquet_all(parquet_column('id')->isNull(), parquet_column('id')->equals('x')),
                Schema::with(FlatColumn::int64('id')),
            ))['filter'] ?? null,
        );
    }

    public function test_the_limit_crosses_beside_the_filter(): void
    {
        static::assertSame(
            ['selectors' => [[4, false]], 'filter' => ['leaf' => 'is_null', 'path' => ['id']], 'limit' => 2],
            ReadPlanConverter::toExtension(ReadPlanMother::rowFiltered(
                4,
                parquet_column('id')->isNull(),
                Schema::with(FlatColumn::int64('id')),
                2,
            )),
        );
    }

    public function test_a_row_filter_holding_an_unbound_leaf_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows are evaluated on bound leaves only, the leaf on "id" is not bound');

        ReadPlanConverter::toExtension(
            new ReadPlan(
                RowSelection::all(4),
                RowSelection::none(4),
                new InMemoryIndexes(),
                BoundFilter::bind(
                    parquet_column('id')->equals('1'),
                    Schema::with(FlatColumn::int64('id')),
                    new Options(),
                ),
            ),
        );
    }

    public function test_the_selection_crosses_as_file_absolute_count_and_skip_pairs(): void
    {
        static::assertSame(
            ['selectors' => [[2, true], [3, false]], 'limit' => null],
            ReadPlanConverter::toExtension(ReadPlan::window(5, new InMemoryIndexes(), 2)),
        );
    }
}
