<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Closure;
use DateTimeImmutable;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Query;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\FilePlan;
use Flow\Parquet\Tests\Context\FilterExpectations;
use Flow\Parquet\Tests\Context\FilterRows;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function array_diff;
use function array_filter;
use function array_map;
use function array_slice;
use function array_values;
use function extension_loaded;
use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;
use function is_float;
use function is_int;
use function iterator_to_array;
use function range;
use function serialize;

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

    /**
     * arrow-rs reads the file's day (INT32 DATE) as Date64 milliseconds and its ts (plain INT64) as Timestamp(Second),
     * as the file's arrow schema hints; seconds (TIMESTAMP(MILLIS) hinted Timestamp(Second), as pyarrow writes it) it
     * reads in milliseconds.
     *
     * @return Generator<string, array{Filter, list<int>}>
     */
    public static function hinted_filters(): Generator
    {
        yield 'date64 at least' => [
            parquet_column('day')->greaterThanOrEqual(new DateTimeImmutable('2024-01-05 UTC')),
            [5, 6, 7, 8, 9],
        ];
        yield 'date64 not in' => [
            parquet_not(parquet_column('day')->in([
                new DateTimeImmutable('2024-01-08 UTC'),
                new DateTimeImmutable('2024-01-02 UTC'),
            ])),
            [0, 2, 3, 5, 6, 8, 9],
        ];
        yield 'pyarrow seconds stored as milliseconds' => [
            parquet_not(parquet_column('seconds')->lessThanOrEqual(new DateTimeImmutable('2024-01-01 04:00:00 UTC'))),
            [5, 6, 7, 8, 9],
        ];
        yield 'seconds below' => [parquet_column('ts')->lessThan(1_704_067_200 + (5 * 3600)), [0, 1, 2, 3, 4]];
        yield 'seconds null or above' => [
            parquet_any(parquet_column('ts')->isNull(), parquet_column('ts')->greaterThan(1_704_067_200 + (7 * 3600))),
            [6, 8, 9],
        ];
    }

    /**
     * Row-level cases: every operator and its negation over NaN and -0.0, nulls, a struct leaf under null structs, a
     * decimal, a timestamp, an absent column, and conjuncts bound beside one that is not.
     *
     * @return Generator<string, array{string, Filter}>
     */
    public static function row_filters(): Generator
    {
        $filters = [
            'NaN equal' => parquet_column('score')->equals(5.0),
            'NaN not equal' => parquet_column('score')->notEquals(5.0),
            'NaN less than' => parquet_column('score')->lessThan(15.0),
            'NaN not less than' => parquet_not(parquet_column('score')->lessThan(15.0)),
            'NaN less than or equal' => parquet_column('score')->lessThanOrEqual(15.0),
            'NaN not less than or equal' => parquet_not(parquet_column('score')->lessThanOrEqual(15.0)),
            'NaN greater than' => parquet_column('score')->greaterThan(15.0),
            'NaN not greater than' => parquet_not(parquet_column('score')->greaterThan(15.0)),
            'NaN greater than or equal' => parquet_column('score')->greaterThanOrEqual(15.0),
            'NaN not greater than or equal' => parquet_not(parquet_column('score')->greaterThanOrEqual(15.0)),
            'NaN in' => parquet_column('score')->in([1.0, 33.0]),
            'NaN not in' => parquet_column('score')->notIn([1.0, 33.0]),
            'negative zero equal' => parquet_column('score')->equals(-0.0),
            'negative zero not greater than' => parquet_not(parquet_column('score')->greaterThan(0.0)),
            'nulls under not and any' => parquet_not(parquet_any(
                parquet_column('maybe')->lessThan(14),
                parquet_column('maybe')->isNull(),
            )),
            'struct leaf under null structs' => parquet_not(parquet_column('address.city')->equals('c3')),
            'struct leaf is null' => parquet_column('address.city')->isNull(),
            'decimal' => parquet_not(parquet_column('amount')->greaterThan(12.25)),
            'timestamp' => parquet_column('ts')->greaterThan(new DateTimeImmutable('2024-01-02 09:00:00.123456 UTC')),
            'absent column' => parquet_not(parquet_column('missing')->equals(1)),
            'absent column is not null' => parquet_column('missing')->isNotNull(),
            'bound beside unbound' => parquet_all(
                parquet_column('id')->lessThan(25),
                parquet_column('big')->lessThan(1.0e18),
                parquet_not(parquet_column('score')->lessThan(5.0)),
            ),
        ];

        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach ($filters as $name => $filter) {
                yield $fixture . ': ' . $name => [$fixture, $filter];
            }
        }
    }

    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('arrow is not loaded');
        }
    }

    #[DataProvider('row_filters')]
    public function test_both_engines_read_exactly_the_rows_the_row_filter_keeps(string $fixture, Filter $filter): void
    {
        $all = Reader::php()->read(self::FIXTURES . $fixture);
        $columns = ['id', 'score', 'maybe', 'address'];
        $rows = [];

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture);
            $plan = FilePlan::of($file, $filter);
            $rowFilter = $plan->rowFilter ?? static::fail('The filter has no bound conjunct');
            $rows[$engine] = FilePlan::rows($file, $columns, $plan);

            static::assertSame(
                FilterExpectations::matchingIds($all->values(), $rowFilter->filter(), $all->schema()),
                FilterExpectations::ids($rows[$engine]),
            );
            static::assertSame(
                [],
                array_diff(
                    FilterExpectations::matchingIds($all->values(), $filter, $all->schema()),
                    FilterExpectations::ids($rows[$engine]),
                ),
            );
        }

        // serialized: NaN equals itself and -0.0 differs from +0.0
        static::assertSame(serialize($rows[PhpParquetEngine::class]), serialize($rows[RustParquetEngine::class]));
    }

    /**
     * @param list<int> $ids
     */
    #[DataProvider('hinted_filters')]
    public function test_both_engines_filter_the_columns_an_arrow_schema_hints_alike(Filter $filter, array $ids): void
    {
        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES
            . 'row_filter_hints_arrow.parquet');

            static::assertSame(
                $ids,
                FilterExpectations::ids(FilePlan::rows($file, ['id'], FilePlan::of($file, $filter))),
            );
        }
    }

    public function test_an_unbound_conjunct_is_left_out_of_the_row_filter_and_read_as_a_superset(): void
    {
        $bound = parquet_column('id')->lessThan(25);
        // a float beyond 2^53 has no exact pre-image on UINT_64: the conjunct is unbound, FilterEvaluator still answers it
        $filter = parquet_all($bound, parquet_column('big')->lessThan(1.0e18));

        foreach (['row_groups_php.parquet', 'row_groups_arrow.parquet'] as $fixture) {
            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture);
                $plan = FilePlan::of($file, $filter);

                static::assertSame($bound, $plan->rowFilter?->filter());
                static::assertSame(range(0, 24), FilterExpectations::ids(FilePlan::rows($file, ['id'], $plan)));
                static::assertSame(range(0, 19), FilterExpectations::ids($file->values(new Query(['id'], $filter))));
            }
        }
    }

    public function test_both_engines_filter_nanosecond_timestamps_by_the_microsecond_they_decode_to(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'), FlatColumnMother::timestamp(TimeUnit::NANOSECONDS));
        $rows = array_map(
            static fn(int $id): array => [
                'id' => $id,
                'ts' => (new DateTimeImmutable('2024-01-01 00:00:00.000001 UTC'))->modify('+' . $id . ' microseconds'),
            ],
            range(0, 9),
        );
        $filter = parquet_any(
            parquet_column('ts')->equals(new DateTimeImmutable('2024-01-01 00:00:00.000003 UTC')),
            parquet_column('ts')->greaterThan(new DateTimeImmutable('2024-01-01 00:00:00.000008 UTC')),
        );

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $writer) {
            $bytes = MemoryParquetFile::written(
                new Writer(engine: ParquetEngineMother::create($writer)),
                $schema,
                $rows,
            );

            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                $file = MemoryParquetFile::read(new Reader(engine: ParquetEngineMother::create($engine)), $bytes);

                static::assertSame(
                    [2, 8, 9],
                    FilterExpectations::ids(FilePlan::rows($file, ['id'], FilePlan::of($file, $filter))),
                );
            }
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

        static::assertEquals($plans[PhpParquetEngine::class]->selection, $plans[RustParquetEngine::class]->selection);
        static::assertEquals($plans[PhpParquetEngine::class]->matched, $plans[RustParquetEngine::class]->matched);
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

        static::assertEquals($plans[PhpParquetEngine::class]->selection, $plans[RustParquetEngine::class]->selection);
        static::assertEquals($plans[PhpParquetEngine::class]->matched, $plans[RustParquetEngine::class]->matched);
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

    /**
     * Pages of the parquet-mr file start at other rows per column: int64 at 0, 200, 428, 628, 828; float at 0, 300,
     * 628, 928. Each filter keeps pages the other column's boundaries cut through.
     *
     * @return Generator<string, array{Filter, RowSelection, Closure(array<string, mixed>): bool}>
     */
    public static function parquet_mr_page_filters(): Generator
    {
        yield 'int64 range' => [
            parquet_column('int64_column')->lessThan(300),
            new RowSelection(new RowSelector(428, false), new RowSelector(572, true)),
            static fn(array $row): bool => is_int($row['int64_column']) && $row['int64_column'] < 300,
        ];
        yield 'float range' => [
            parquet_column('float_column')->lessThan(400.0),
            new RowSelection(new RowSelector(628, false), new RowSelector(372, true)),
            static fn(array $row): bool => is_float($row['float_column']) && $row['float_column'] < 400.0,
        ];
    }

    /**
     * Even ids only, so every odd id lies inside a group's min/max: only a bloom filter drops its group.
     *
     * @return Generator<string, array{string, Filter, RowSelection, list<int>}>
     */
    public static function bloom_filters(): Generator
    {
        foreach (['bloom_php.parquet', 'bloom_arrow.parquet'] as $fixture) {
            yield $fixture . ': absent id' => [$fixture, parquet_column('id')->equals(33), RowSelection::none(40), []];
            yield $fixture . ': in, present and absent' => [
                $fixture,
                parquet_column('id')->in([24, 33]),
                new RowSelection(new RowSelector(10, true), new RowSelector(10, false), new RowSelector(20, true)),
                [24],
            ];
            yield $fixture . ': present key' => [
                $fixture,
                parquet_column('key')->equals('k50'),
                new RowSelection(new RowSelector(20, true), new RowSelector(10, false), new RowSelector(10, true)),
                [50],
            ];
        }

        yield 'bloom_duckdb.parquet: absent id' => [
            'bloom_duckdb.parquet',
            parquet_column('id')->equals(40_001),
            RowSelection::none(FilterRows::DUCKDB_BLOOM_ROWS),
            [],
        ];
        yield 'bloom_duckdb.parquet: in, present and absent' => [
            'bloom_duckdb.parquet',
            parquet_column('id')->in([40_000, 40_001]),
            new RowSelection(new RowSelector(20_480, false), new RowSelector(79_520, true)),
            [40_000],
        ];
        yield 'bloom_duckdb.parquet: present key' => [
            'bloom_duckdb.parquet',
            parquet_column('key')->equals('k100000'),
            new RowSelection(
                new RowSelector(40_960, true),
                new RowSelector(20_480, false),
                new RowSelector(38_560, true),
            ),
            [100_000],
        ];
    }

    /**
     * @return Generator<string, array{string, Filter, RowSelection, RowSelection, list<int>}>
     */
    public static function page_filters(): Generator
    {
        foreach (['pages_php.parquet', 'pages_arrow.parquet'] as $fixture) {
            yield $fixture . ': string prefix' => [
                $fixture,
                parquet_column('label')->startsWith('10'),
                new RowSelection(new RowSelector(10, true), new RowSelector(30, false)),
                new RowSelection(new RowSelector(10, true), new RowSelector(20, false), new RowSelector(10, true)),
                [...range(10, 29), 31, 32, 34, 35, 37, 38],
            ];
            yield $fixture . ': not less than on NaN' => [
                $fixture,
                parquet_not(parquet_column('score')->lessThan(100.0)),
                new RowSelection(
                    new RowSelector(10, true),
                    new RowSelector(5, false),
                    new RowSelector(5, true),
                    new RowSelector(10, false),
                    new RowSelector(10, true),
                ),
                new RowSelection(new RowSelector(20, true), new RowSelector(10, false), new RowSelector(10, true)),
                [11, ...range(20, 29)],
            ];
        }
    }

    /**
     * @param Closure(array<string, mixed>): bool $matches
     */
    #[DataProvider('parquet_mr_page_filters')]
    public function test_both_engines_page_prune_a_file_whose_columns_break_pages_at_other_rows(
        Filter $filter,
        RowSelection $selection,
        Closure $matches,
    ): void {
        $fixture = __DIR__ . '/../IO/Fixtures/delta_binary_acked_encoded_integers.parquet';
        $columns = ['int32_column', 'int64_column', 'float_column'];
        $all = iterator_to_array(Reader::php()->read($fixture)->values(), false);
        $rows = [];

        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read($fixture);
            $plan = FilePlan::of($file, $filter);

            static::assertEquals($selection, $plan->selection);
            $rows[$engine] = FilePlan::rows($file, $columns, $plan);
            static::assertSame(
                array_values(array_filter($all, $matches)),
                iterator_to_array($file->values(new Query($columns, $filter)), false),
            );
        }

        static::assertSame($rows[PhpParquetEngine::class], $rows[RustParquetEngine::class]);
        static::assertSame(array_values(array_filter($all, $matches)), $rows[PhpParquetEngine::class]);
    }

    /**
     * @param list<int> $ids
     */
    #[DataProvider('bloom_filters')]
    public function test_both_engines_skip_the_row_groups_whose_bloom_filters_lack_every_literal(
        string $fixture,
        Filter $filter,
        RowSelection $selection,
        array $ids,
    ): void {
        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture);

            static::assertEquals($selection, FilePlan::of($file, $filter)->selection);
            static::assertSame($ids, FilterExpectations::ids($file->values(new Query(['id'], $filter))));
        }
    }

    /**
     * @param list<int> $ids
     */
    #[DataProvider('page_filters')]
    public function test_both_engines_select_and_match_the_same_pages(
        string $fixture,
        Filter $filter,
        RowSelection $selection,
        RowSelection $matched,
        array $ids,
    ): void {
        foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
            $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture);
            $plan = FilePlan::of($file, $filter);

            static::assertEquals($selection, $plan->selection);
            static::assertEquals($matched, $plan->matched);
            static::assertSame($ids, FilterExpectations::ids($file->values(new Query(['id'], $filter))));
        }
    }

    public function test_both_engines_trim_a_list_inside_a_struct_to_the_selected_rows_of_a_page(): void
    {
        $expected = array_map(
            static fn(array $row): array => ['id' => $row['id'], 'address' => $row['address']],
            array_slice(FilterRows::pageRows(), 12),
        );

        foreach (['pages_php.parquet', 'pages_arrow.parquet'] as $fixture) {
            foreach ([PhpParquetEngine::class, RustParquetEngine::class] as $engine) {
                $file = (new Reader(engine: ParquetEngineMother::create($engine)))->read(self::FIXTURES . $fixture);
                $plan = FilePlan::of($file, parquet_column('id')->greaterThanOrEqual(12), 12);

                static::assertEquals(RowSelection::window(40, 12), $plan->selection);
                static::assertEquals($expected, FilePlan::rows($file, ['id', 'address'], $plan));
            }
        }
    }
}
