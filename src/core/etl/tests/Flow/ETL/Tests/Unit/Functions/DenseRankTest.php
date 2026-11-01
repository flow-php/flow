<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Tests\Context\AggregatorRoundTrip;
use Flow\ETL\Tests\Context\RankingContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class DenseRankTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        [$original, $copy] = AggregatorRoundTrip::windowed(dense_rank()->over(
            window()->partitionBy(ref('d'))->orderBy(ref('v')->desc()),
        ));

        static::assertSame($original, $copy);
    }

    public function test_dense_rank_over_ascending_order(): void
    {
        static::assertSame(
            [1, 2, 3, 3, 3],
            dense_rank()
                ->over(window()->orderBy(ref('salary')))
                ->values(RankingContext::salariesAscending(), flow_context()->backend()),
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
                ->values(RankingContext::salariesDescending(), flow_context()->backend()),
        );
    }

    public function test_dense_rank_over_multiple_order_columns(): void
    {
        static::assertSame(
            [1, 2, 3, 4, 5],
            dense_rank()
                ->over(window()->orderBy(ref('salary')->desc(), ref('id')))
                ->values(RankingContext::salariesDescending(), flow_context()->backend()),
        );
    }

    public function test_dense_rank_requires_an_order_by(): void
    {
        $this->expectExceptionMessage('Dens Rank window function requires to be ordered by one column');

        dense_rank()
            ->over(window())
            ->values(array_to_rows([['salary' => 6000]], schema(int_schema('salary'))), flow_context()->backend());
    }

    public function test_dense_rank_without_over_clause(): void
    {
        $this->expectExceptionMessage('Window function "dens_rank()" requires an OVER clause.');

        dense_rank()->values(RankingContext::salariesDescending(), flow_context()->backend());
    }

    public function test_with_children_returns_the_same_leaf(): void
    {
        $function = dense_rank();

        static::assertSame([], $function->children());
        static::assertSame($function, $function->withChildren([]));
    }
}
