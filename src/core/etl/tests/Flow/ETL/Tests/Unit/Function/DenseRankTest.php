<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\RankingContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class DenseRankTest extends FlowTestCase
{
    public function test_apply_returns_the_rank_at_the_row_index(): void
    {
        $partition = RankingContext::salariesDescending();
        $function = dense_rank()->over(window()->orderBy(ref('salary')->desc()));

        $applied = [];

        for ($index = 0; $index < $partition->count(); $index++) {
            $applied[] = $function->apply(WindowContextMother::atIndex($partition, $index));
        }

        static::assertSame($function->rankPartition($partition), $applied);
    }

    /**
     * PostgreSQL parity. Before this was a single pass over the sorted partition, dense_rank() ignored
     * the ORDER BY direction and always ranked descending, so the lowest salary got rank 3 here.
     */
    public function test_dense_rank_over_ascending_order(): void
    {
        static::assertSame(
            [1, 2, 3, 3, 3],
            dense_rank()->over(window()->orderBy(ref('salary')))->rankPartition(RankingContext::salariesAscending()),
        );
    }

    /**
     * Unlike rank(), dense_rank() does not leave gaps after a tie.
     */
    public function test_dense_rank_over_descending_order(): void
    {
        static::assertSame(
            [1, 1, 1, 2, 3],
            dense_rank()
                ->over(window()->orderBy(ref('salary')->desc()))
                ->rankPartition(RankingContext::salariesDescending()),
        );
    }

    public function test_dense_rank_over_multiple_order_columns(): void
    {
        static::assertSame(
            [1, 2, 3, 4, 5],
            dense_rank()
                ->over(window()->orderBy(ref('salary')->desc(), ref('id')))
                ->rankPartition(RankingContext::salariesDescending()),
        );
    }

    public function test_dense_rank_requires_an_order_by(): void
    {
        $this->expectExceptionMessage('Dens Rank window function requires to be ordered by one column');

        dense_rank()->over(window())->rankPartition(rows(schema(int_schema('salary')), row(['salary' => 6000])));
    }

    public function test_dense_rank_without_over_clause(): void
    {
        $this->expectExceptionMessage('Window function "dens_rank()" requires an OVER clause.');

        $partition = RankingContext::salariesDescending();

        dense_rank()->apply(WindowContextMother::atIndex($partition, 0));
    }

    public function test_with_children_returns_the_same_leaf(): void
    {
        $function = dense_rank();

        static::assertSame([], $function->children());
        static::assertSame($function, $function->withChildren([]));
    }
}
