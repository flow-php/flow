<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use DateTimeImmutable;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Query;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\FilePlan;
use Flow\Parquet\Tests\Context\FilterExpectations;
use Flow\Parquet\Tests\Context\FilterRows;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function array_diff;
use function extension_loaded;
use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;
use function range;

#[Group('native-extension')]
final class FilterParityTest extends TestCase
{
    public const string FIXTURES = __DIR__ . '/Fixtures/Filters/';

    /**
     * Each filter, the raw rows a plan skips first, the row groups of 10 rows FilterRows keeps and the ids of the rows
     * matching it, worked out from the data FilterRows writes.
     *
     * @return Generator<string, array{Filter, int, list<int>, list<int>}>
     */
    public static function filters(): Generator
    {
        yield 'equal' => [parquet_column('id')->equals(7), 0, [0], [7]];
        yield 'less than' => [parquet_column('id')->lessThan(5), 0, [0], range(0, 4)];
        yield 'less than or equal' => [parquet_column('id')->lessThanOrEqual(9), 0, [0], range(0, 9)];
        yield 'greater than' => [parquet_column('id')->greaterThan(30), 0, [3], range(31, 39)];
        yield 'greater than or equal' => [parquet_column('id')->greaterThanOrEqual(35), 0, [3], range(35, 39)];
        yield 'in' => [parquet_column('id')->in([3, 33]), 0, [0, 3], [3, 33]];
        yield 'mixed in' => [parquet_column('id')->in([3, 33.0, 7]), 0, [0, 3], [3, 7, 33]];
        yield 'not less than' => [parquet_not(parquet_column('id')->lessThan(20)), 0, [2, 3], range(20, 39)];
        yield 'not equals on NaN' => [
            parquet_column('score')->notEquals(5.0),
            0,
            [0, 1, 2, 3],
            [...range(0, 4), ...range(6, 39)],
        ];
        yield 'not in on NaN' => [
            parquet_column('score')->notIn([1.0, 2.0]),
            0,
            [0, 1, 2, 3],
            [0, ...range(3, 39)],
        ];
        yield 'not less than on NaN' => [
            parquet_not(parquet_column('score')->lessThan(100.0)),
            0,
            [1, 2],
            [11, ...range(20, 29)],
        ];
        yield 'equal to zero holds negative zero' => [parquet_column('score')->equals(0.0), 0, [0, 1], [0, 10]];
        yield 'string range in byte order' => [parquet_column('label')->lessThan('10'), 0, [], []];
        yield 'starts with' => [
            parquet_column('label')->startsWith('10'),
            0,
            [1, 2, 3],
            [...range(10, 29), 31, 32, 34, 35, 37, 38],
        ];
        yield 'starts with a non-ASCII prefix' => [
            parquet_column('word')->startsWith('ź'),
            0,
            [1, 3],
            [...range(10, 19), 31, 34, 37],
        ];
        yield 'is null' => [parquet_column('maybe')->isNull(), 0, [0, 1], [...range(0, 9), 11, 13, 15, 17, 19]];
        yield 'is not null' => [
            parquet_column('maybe')->isNotNull(),
            0,
            [1, 2, 3],
            [10, 12, 14, 16, 18, ...range(20, 39)],
        ];
        yield 'struct leaf' => [parquet_column('address.city')->equals('c2'), 0, [2], range(20, 29)];
        yield 'negated struct leaf under null structs' => [
            parquet_not(parquet_column('address.city')->equals('c3')),
            0,
            [0, 1, 2],
            range(0, 29),
        ];
        yield 'absent column' => [parquet_column('missing')->equals(1), 0, [], []];
        yield 'absent column is null' => [parquet_column('missing')->isNull(), 0, [0, 1, 2, 3], range(0, 39)];
        yield 'date' => [
            parquet_column('day')->greaterThan(new DateTimeImmutable('2024-01-25 UTC')),
            0,
            [2, 3],
            range(25, 39),
        ];
        yield 'date off midnight' => [
            parquet_column('day')->equals(new DateTimeImmutable('2024-01-03 12:00 UTC')),
            0,
            [],
            [],
        ];
        yield 'timestamp' => [
            parquet_column('ts')->lessThanOrEqual(new DateTimeImmutable('2024-01-01 05:00 UTC')),
            0,
            [0],
            range(0, 4),
        ];
        yield 'decimal' => [parquet_column('amount')->equals(12.25), 0, [1], [12]];
        yield 'uint64 above a digit string' => [
            parquet_column('big')->greaterThan('18446744073709551600'),
            0,
            [2, 3],
            range(25, 39),
        ];
        yield 'uint64 equal to an int' => [parquet_column('big')->equals(5), 0, [0], [5]];
        yield 'uint64 against a float' => [
            parquet_column('big')->greaterThanOrEqual(10.0),
            0,
            [1, 2, 3],
            range(10, 39),
        ];
        yield 'uint64 below a fraction' => [parquet_column('big')->lessThan(2.5), 0, [0], [0, 1, 2]];
        yield 'uuid' => [parquet_column('uid')->equals('00000001-0000-4000-8000-00000000000d'), 0, [1], [13]];
        yield 'boolean' => [
            parquet_column('flag')->equals(true),
            0,
            [1, 2, 3],
            [...range(10, 19), 20, 22, 24, 26, 28, 30, 32, 34, 36, 38],
        ];
        yield 'float32 equal' => [parquet_column('ratio')->equals(20.5), 0, [2], [20]];
        yield 'float32 above' => [parquet_column('ratio')->greaterThan(37.0), 0, [3], [37, 38, 39]];
        yield 'any' => [
            parquet_any(parquet_column('id')->equals(1), parquet_column('label')->equals('100')),
            0,
            [0, 2, 3],
            [1, ...range(20, 29), 32, 35, 38],
        ];
        yield 'all' => [
            parquet_all(parquet_column('id')->greaterThan(10), parquet_column('maybe')->isNull()),
            0,
            [1],
            [11, 13, 15, 17, 19],
        ];
        yield 'offset with a filter' => [parquet_column('id')->greaterThanOrEqual(15), 25, [2, 3], range(15, 39)];
        yield 'offset inside a kept group, then a pruned group' => [
            parquet_column('id')->in([3, 33]),
            5,
            [0, 3],
            [3, 33],
        ];
    }

    /**
     * @return Generator<string, array{string, Filter, int, list<int>}>
     */
    public static function fixtures_and_filters(): Generator
    {
        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach (self::filters() as $name => [$filter, $offset, $kept]) {
                yield $fixture . ': ' . $name => [$fixture, $filter, $offset, $kept];
            }
        }
    }

    /**
     * @return Generator<string, array{string, Filter, list<int>}>
     */
    public static function fixture_filters(): Generator
    {
        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach (self::filters() as $name => [$filter, , , $ids]) {
                yield $fixture . ': ' . $name => [$fixture, $filter, $ids];
            }
        }
    }

    /**
     * @return Generator<string, array{Filter, int}>
     */
    public static function duckdb_filters(): Generator
    {
        foreach (self::filters() as $name => [$filter, $offset]) {
            yield $name => [$filter, $offset];
        }
    }

    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('arrow is not loaded');
        }
    }

    /**
     * @param list<int> $kept
     */
    #[DataProvider('fixtures_and_filters')]
    public function test_both_engines_plan_one_selection_that_keeps_every_matching_row(
        string $fixture,
        Filter $filter,
        int $offset,
        array $kept,
    ): void {
        $all = Reader::php()->read(self::FIXTURES . $fixture);
        $matching = FilterExpectations::matchingGroups(
            $all->values(),
            $filter,
            $all->schema(),
            FilterRows::ROWS_PER_GROUP,
            $offset,
        );
        $plans = [];

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $plans[$engine] = FilePlan::of(
                (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture),
                $filter,
                $offset,
            );
        }

        static::assertEquals($plans[PhpParquetEngine::class], $plans[RustParquetEngine::class]);
        static::assertSame($kept, FilterExpectations::keptGroups(
            $plans[PhpParquetEngine::class]->selection,
            FilterRows::ROWS_PER_GROUP,
        ));
        static::assertSame([], array_diff($matching, $kept));
    }

    /**
     * @param list<int> $ids
     */
    #[DataProvider('fixture_filters')]
    public function test_both_engines_read_exactly_the_matching_rows(string $fixture, Filter $filter, array $ids): void
    {
        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            static::assertSame(
                $ids,
                FilterExpectations::ids(
                    (new Reader(engine: ParquetEngineMother::create($engine)))
                        ->read(self::FIXTURES . $fixture)
                        ->values(new Query(['id'], $filter)),
                ),
            );
        }
    }

    public function test_offset_and_limit_count_the_matching_rows(): void
    {
        $filter = parquet_column('id')->in([3, 7, 33, 35, 38]);

        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                static::assertSame(
                    [7, 33],
                    FilterExpectations::ids(
                        (new Reader(engine: ParquetEngineMother::create($engine)))
                            ->read(self::FIXTURES . $fixture)
                            ->values(new Query(['id'], $filter, offset: 1, limit: 2)),
                    ),
                );
            }
        }
    }

    #[DataProvider('duckdb_filters')]
    public function test_both_engines_keep_every_duckdb_row_group_holding_a_match(Filter $filter, int $offset): void
    {
        $plans = [];

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $plans[$engine] = FilePlan::of(
                (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES
                . 'row_groups_duckdb.parquet'),
                $filter,
                $offset,
            );
        }

        $matching = FilterExpectations::matchingGroups(
            FilterRows::rows(FilterRows::DUCKDB_ROWS_PER_GROUP),
            $filter,
            FilterRows::schema(),
            FilterRows::DUCKDB_ROWS_PER_GROUP,
            $offset,
        );

        static::assertEquals($plans[PhpParquetEngine::class], $plans[RustParquetEngine::class]);
        static::assertSame(
            [],
            array_diff($matching, FilterExpectations::keptGroups(
                $plans[PhpParquetEngine::class]->selection,
                FilterRows::DUCKDB_ROWS_PER_GROUP,
            )),
        );
    }

    public function test_both_engines_read_exactly_the_matching_duckdb_rows_from_the_kept_group(): void
    {
        $filter = parquet_column('id')->lessThan(100);

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES
            . 'row_groups_duckdb.parquet');
            static::assertSame(
                [0],
                FilterExpectations::keptGroups(
                    FilePlan::of($file, $filter)->selection,
                    FilterRows::DUCKDB_ROWS_PER_GROUP,
                ),
            );
            static::assertSame(range(0, 99), FilterExpectations::ids($file->values(new Query(['id'], $filter))));
        }
    }

    public function test_both_engines_keep_every_row_group_of_a_file_without_column_orders(): void
    {
        $fixture = __DIR__ . '/../IO/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        foreach ([parquet_column('id')->equals(1), parquet_column('name')->equals('name-1')] as $filter) {
            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read($fixture);
                static::assertEquals(RowSelection::all(5000), FilePlan::of($file, $filter)->selection);
                static::assertSame([1], FilterExpectations::ids($file->values(new Query(['id'], $filter))));
            }
        }
    }

    public function test_an_offset_from_inside_a_kept_group_reads_its_rest_and_skips_the_pruned_groups(): void
    {
        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                static::assertEquals(
                    new RowSelection(
                        new RowSelector(5, true),
                        new RowSelector(5, false),
                        new RowSelector(20, true),
                        new RowSelector(10, false),
                    ),
                    FilePlan::of(
                        (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture),
                        parquet_column('id')->in([3, 33]),
                        5,
                    )->selection,
                );
            }
        }
    }
}
