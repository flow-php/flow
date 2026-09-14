<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformation;

use function array_column;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_memory;

final class BranchingTest extends FlowIntegrationTestCase
{
    public function test_a_branch_transformation_builds_on_the_columns_it_is_fed(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
            ]))
            ->write(to_branch(
                ref('group')->equals(lit('A')),
                to_memory($memory = new ArrayMemory()),
            )->withTransformation(new CallbackTransformation(static fn(DataFrame $dataFrame): DataFrame => $dataFrame->withEntry(
                'group_name',
                lit('A'),
            ))))
            ->run();

        static::assertSame([['id' => 1, 'group' => 'A', 'group_name' => 'A']], $memory->dump());
    }

    public function test_a_branch_nothing_matches_never_loads_and_still_closes(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_branch(lit(false), $spy)->withTransformation(new CallbackTransformation(
                static fn(DataFrame $dataFrame): DataFrame => $dataFrame->select('id'),
            )))
            ->run();

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_only_the_rows_matching_the_condition_reach_the_branch(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_branch(ref('id')->greaterThanEqual(lit(2)), $spy))
            ->run();

        static::assertSame([2, 2], $spy->loadedRowCounts());
        static::assertSame([5, 4, 3, 2], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_branching(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 5, 'group' => 'A'],
                ['id' => 6, 'group' => 'C'],
            ]))
            ->write(to_branch(ref('group')->equals(lit('A')), to_memory($memoryA = new ArrayMemory())))
            ->write(to_branch(ref('group')->isIn(lit(['B', 'C'])), to_memory($memoryBC = new ArrayMemory())))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'group' => 'A'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 5, 'group' => 'A'],
            ],
            $memoryA->dump(),
        );
        static::assertSame(
            [
                ['id' => 2, 'group' => 'B'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 6, 'group' => 'C'],
            ],
            $memoryBC->dump(),
        );
    }

    public function test_branching_with_aggregate_transformation_answers_once_for_the_stream(): void
    {
        df()
            ->read(from_array([
                ['id' => 5, 'group' => 'A'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
                ['id' => 1, 'group' => 'A'],
                ['id' => 0, 'group' => 'B'],
            ]))
            ->batchSize(2)
            ->write(to_branch(
                ref('group')->equals(lit('A')),
                to_memory($memoryA = new ArrayMemory()),
            )->withTransformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->aggregate([sum(ref('id'))]);
                }
            }))
            ->run();

        static::assertSame([['id_sum' => 9.0]], $memoryA->dump());
    }

    public function test_branch_rejects_a_non_boolean_condition_at_bind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('filter() requires a predicate returning boolean');

        df()
            ->read(from_array([['id' => 1, 'group' => 'A']]))
            ->write(to_branch(ref('id'), to_memory(new ArrayMemory())))
            ->run();
    }

    public function test_branching_with_blocking_transformation_sorts_the_whole_branch(): void
    {
        df()
            ->read(from_array([
                ['id' => 5, 'group' => 'A'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
                ['id' => 1, 'group' => 'A'],
                ['id' => 0, 'group' => 'B'],
            ]))
            ->batchSize(2)
            ->write(to_branch(
                ref('group')->equals(lit('A')),
                to_memory($memoryA = new ArrayMemory()),
            )->withTransformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->sortBy([ref('id')]);
                }
            }))
            ->run();

        static::assertSame(
            [['id' => 1, 'group' => 'A'], ['id' => 3, 'group' => 'A'], ['id' => 5, 'group' => 'A']],
            $memoryA->dump(),
        );
    }

    public function test_branching_with_transformation(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 5, 'group' => 'A'],
                ['id' => 6, 'group' => 'C'],
            ]))
            ->write(to_branch(
                ref('group')->equals(lit('A')),
                to_memory($memoryA = new ArrayMemory()),
            )->withTransformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('group_name', lit('A'));
                }
            }))
            ->write(to_branch(
                ref('group')->isIn(lit(['B', 'C'])),
                to_memory($memoryBC = new ArrayMemory()),
            )->withTransformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('group_name', lit('BC'));
                }
            }))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'group' => 'A', 'group_name' => 'A'],
                ['id' => 3, 'group' => 'A', 'group_name' => 'A'],
                ['id' => 5, 'group' => 'A', 'group_name' => 'A'],
            ],
            $memoryA->dump(),
        );
        static::assertSame(
            [
                ['id' => 2, 'group' => 'B', 'group_name' => 'BC'],
                ['id' => 4, 'group' => 'B', 'group_name' => 'BC'],
                ['id' => 6, 'group' => 'C', 'group_name' => 'BC'],
            ],
            $memoryBC->dump(),
        );
    }
}
