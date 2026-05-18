<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Window;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
use function iterator_to_array;

final class WindowFunctionsTest extends FlowTestCase
{
    public function test_rank_on_partitioned_window(): void
    {
        $rows = data_frame()
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
            ->withEntry(
                'rank',
                dense_rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())),
            )
            ->sortBy(ref('department'), ref('rank'))
            ->get();

        static::assertEquals(
            [
                ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000, 'rank' => 1],
                ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000, 'rank' => 2],
                ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000, 'rank' => 3],
                ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000, 'rank' => 4],
                ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000, 'rank' => 1],
                ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000, 'rank' => 2],
            ],
            array_merge(...array_map(static fn(Rows $r) => $r->toArray(), iterator_to_array($rows, false))),
        );
    }

    public function test_rank_without_partitioning(): void
    {
        $rows = data_frame()
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
            ->withEntry('rank', rank()->over(window()->orderBy(ref('salary')->desc())))
            ->get();

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
            array_map(static fn(Rows $r) => $r->toArray(), iterator_to_array($rows, false)),
        );
    }
}
