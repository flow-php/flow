<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\RankingContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\window;

final class RankTest extends FlowTestCase
{
    public function test_apply_returns_the_rank_at_the_row_index(): void
    {
        $partition = RankingContext::salariesDescending();
        $function = rank()->over(window()->orderBy(ref('salary')->desc()));

        $applied = [];

        for ($index = 0; $index < $partition->count(); $index++) {
            $applied[] = $function->apply(WindowContextMother::atIndex($partition, $index));
        }

        static::assertSame($function->rankPartition($partition), $applied);
    }

    /**
     * PostgreSQL parity. Before this was a single pass over the sorted partition, rank() ignored the
     * ORDER BY direction and always ranked descending, so the lowest salary got rank 5 here.
     */
    public function test_rank_over_ascending_order(): void
    {
        static::assertSame(
            [1, 2, 3, 3, 3],
            rank()->over(window()->orderBy(ref('salary')))->rankPartition(RankingContext::salariesAscending()),
        );
    }

    public function test_rank_over_descending_order(): void
    {
        static::assertSame(
            [1, 1, 1, 4, 5],
            rank()->over(window()->orderBy(ref('salary')->desc()))->rankPartition(RankingContext::salariesDescending()),
        );
    }

    /**
     * Peers must match on every ORDER BY column, so adding a distinct second column breaks every tie.
     */
    public function test_rank_over_multiple_order_columns(): void
    {
        static::assertSame(
            [1, 2, 3, 4, 5],
            rank()
                ->over(window()->orderBy(ref('salary')->desc(), ref('id')))
                ->rankPartition(RankingContext::salariesDescending()),
        );
    }

    public function test_rank_requires_an_order_by(): void
    {
        $this->expectExceptionMessage('Rank window function requires to be ordered by one column');

        rank()->over(window())->rankPartition(rows(row(int_entry('salary', 6000))));
    }

    public function test_rank_without_over_clause(): void
    {
        $this->expectExceptionMessage('Window function "rank()" requires an OVER clause.');

        $partition = RankingContext::salariesDescending();

        rank()->apply(WindowContextMother::atIndex($partition, 0));
    }
}
