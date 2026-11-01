<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Window;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Tests\Context\WindowFrameContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\unbounded_following;
use function Flow\ETL\DSL\unbounded_preceding;
use function Flow\ETL\DSL\window;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class WindowFunctionsTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{AggregatingFunction&WindowFunction, int|float}>
     */
    public static function frame_aggregates_over_nulls(): Generator
    {
        yield 'count' => [count(ref('x')), 2];
        yield 'sum' => [sum(ref('x')), 4.0];
        yield 'average' => [average(ref('x')), 2.0];
    }

    #[DataProvider('frame_aggregates_over_nulls')]
    public function test_a_window_aggregate_equals_the_group_by_aggregate_over_nulls(
        AggregatingFunction&WindowFunction $function,
        int|float $expected,
    ): void {
        $rows = [['g' => 'a', 'x' => 1], ['g' => 'a', 'x' => null], ['g' => 'a', 'x' => 3]];

        static::assertSame(
            [$expected],
            data_frame()
                ->read(from_array($rows))
                ->groupBy('g')
                ->aggregate($function)
                ->fetch()
                ->reduceToArray($function->outputName()),
        );
        static::assertSame(
            [$expected, $expected, $expected],
            data_frame()
                ->read(from_array($rows))
                ->withColumn('w', $function->over(window()->partitionBy(ref('g'))))
                ->fetch()
                ->reduceToArray('w'),
        );
    }

    /**
     * @return Generator<string, array{AggregatingFunction&WindowFunction}>
     */
    public static function frame_aggregates_of_a_missing_column(): Generator
    {
        yield 'count' => [count(ref('missing'))];
        yield 'sum' => [sum(ref('missing'))];
        yield 'average' => [average(ref('missing'))];
    }

    #[DataProvider('frame_aggregates_of_a_missing_column')]
    public function test_a_window_aggregate_of_a_missing_column_is_rejected_before_it_runs(AggregatingFunction&WindowFunction $function): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found.');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->withColumn('w', $function->over(window()))
            ->fetch();
    }

    public function test_rank_on_partitioned_window(): void
    {
        static::assertEquals(
            [
                ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000, 'rank' => 1],
                ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000, 'rank' => 2],
                ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000, 'rank' => 3],
                ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000, 'rank' => 4],
                ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000, 'rank' => 1],
                ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000, 'rank' => 2],
            ],
            array_merge(...array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array(
                    data_frame()
                        ->read(from_all(
                            from_array([
                                ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000],
                                ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000],
                                ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000],
                            ]),
                            from_array([
                                ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000],
                                ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000],
                                ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000],
                            ]),
                        ))
                        ->withColumn(
                            'rank',
                            dense_rank()->over(
                                window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc()),
                            ),
                        )
                        ->sortBy([ref('department'), ref('rank')])
                        ->get(),
                    false,
                ),
            )),
        );
    }

    public function test_rank_without_partitioning(): void
    {
        static::assertSame(
            [
                [
                    ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000, 'rank' => 1],
                    ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000, 'rank' => 2],
                    ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000, 'rank' => 3],
                    ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000, 'rank' => 4],
                    ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000, 'rank' => 5],
                    ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000, 'rank' => 6],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array(
                    data_frame()
                        ->read(from_all(
                            from_array([
                                ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000],
                                ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000],
                                ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000],
                            ]),
                            from_array([
                                ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000],
                                ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000],
                                ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000],
                            ]),
                        ))
                        ->withColumn('rank', rank()->over(window()->orderBy(ref('salary')->desc())))
                        ->get(),
                    false,
                ),
            ),
        );
    }

    public function test_centered_frame(): void
    {
        static::assertSame(
            [150.0, 200.0, 300.0, 400.0, 450.0],
            WindowFrameContext::salaries(
                average(ref('salary'))
                    ->over(
                        window()
                            ->partitionBy(ref('department'))
                            ->orderBy(ref('date'))
                            ->rowsBetween(preceding(1), following(1)),
                    ),
            ),
        );
    }

    public function test_default_frame_is_peer_aware(): void
    {
        static::assertSame(
            [300.0, 300.0, 600.0, 1000.0],
            WindowFrameContext::tiedDates(sum(ref('salary'))->over(window()->orderBy(ref('date')))),
        );
    }

    public function test_default_frame_produces_a_running_total(): void
    {
        static::assertSame(
            [100.0, 300.0, 600.0, 1000.0, 1500.0],
            WindowFrameContext::salaries(
                sum(ref('salary'))->over(window()->partitionBy(ref('department'))->orderBy(ref('date'))),
            ),
        );
    }

    public function test_moving_average_over_a_trailing_frame(): void
    {
        static::assertSame(
            [100.0, 150.0, 200.0, 300.0, 400.0],
            WindowFrameContext::salaries(
                average(ref('salary'))
                    ->over(
                        window()
                            ->partitionBy(ref('department'))
                            ->orderBy(ref('date'))
                            ->rowsBetween(preceding(2), current_row()),
                    ),
            ),
        );
    }

    public function test_unbounded_frame_covers_the_whole_partition(): void
    {
        static::assertSame(
            [1500.0, 1500.0, 1500.0, 1500.0, 1500.0],
            WindowFrameContext::salaries(
                sum(ref('salary'))
                    ->over(
                        window()
                            ->partitionBy(ref('department'))
                            ->orderBy(ref('date'))
                            ->rowsBetween(unbounded_preceding(), unbounded_following()),
                    ),
            ),
        );
    }

    public function test_window_partitions_on_datetime_keep_full_precision(): void
    {
        $rows = data_frame()
            ->read(from_array([
                ['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'amount' => 1],
                ['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'amount' => 2],
                ['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'amount' => 3],
                ['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'amount' => 4],
            ]))
            ->withColumn('n', sum(ref('amount'))->over(window()->partitionBy(ref('at'))->orderBy(ref('amount'))))
            ->fetch()
            ->toArray();

        $sums = [];

        foreach ($rows as $row) {
            $sums[type_datetime()->assert($row['at'])->format('H:i') . '/' . type_integer()->assert($row['amount'])] =
                $row['n'];
        }

        static::assertSame(['09:00/1' => 1.0, '09:00/3' => 4.0, '21:00/2' => 2.0, '21:00/4' => 6.0], $sums);
    }

    /**
     * B12: the partition key used to close on change, so a key that reappeared after another key
     * produced two partial partitions instead of one.
     */
    public function test_window_partitions_survive_interleaved_input(): void
    {
        $rows = data_frame()
            ->read(from_array([
                ['region' => 'eu', 'amount' => 1],
                ['region' => 'us', 'amount' => 10],
                ['region' => 'eu', 'amount' => 2],
                ['region' => 'us', 'amount' => 20],
                ['region' => 'eu', 'amount' => 3],
            ]))
            ->withColumn('total', sum(ref('amount'))->over(window()->partitionBy(ref('region'))))
            ->fetch()
            ->toArray();

        $totals = [];

        foreach ($rows as $row) {
            $totals[type_string()->assert($row['region'])] = $row['total'];
        }

        static::assertSame(['eu' => 6.0, 'us' => 30.0], $totals);
    }

    public function test_over_does_not_leak_a_window_across_pipelines(): void
    {
        $window = window()->partitionBy(ref('region'));
        $first = sum(ref('v'))->over($window);
        $second = sum(ref('v'))->over($window);

        $window->partitionBy(ref('region'), ref('country'));

        static::assertNotSame($first, $second);
        static::assertCount(1, $first->window()->partitions()->all());
        static::assertCount(1, $second->window()->partitions()->all());

        $bare = sum(ref('v'));
        $overA = $bare->over(window()->partitionBy(ref('a')));
        $overB = $bare->over(window()->partitionBy(ref('b')));

        static::assertNotSame($bare, $overA);
        static::assertNotSame($overA, $overB);
        static::assertSame('a', $overA->window()->partitions()->all()[0]->name());
        static::assertSame('b', $overB->window()->partitions()->all()[0]->name());
    }
}
