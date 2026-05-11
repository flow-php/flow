<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\window;

final class DenseRankTest extends FlowTestCase
{
    public function test_rank_function_on_collection_of_rows_sorted_by_id_descending(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            $row2 = row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            $row3 = row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            $row4 = row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            $row5 = row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $denseRank = dense_rank()->over(window()->orderBy(ref('salary')->desc()));
        $context = flow_context();

        static::assertSame(1, $denseRank->apply($row1, $rows, $context));
        static::assertSame(1, $denseRank->apply($row2, $rows, $context));
        static::assertSame(1, $denseRank->apply($row3, $rows, $context));
        static::assertSame(3, $denseRank->apply($row4, $rows, $context));
        static::assertSame(2, $denseRank->apply($row5, $rows, $context));
    }

    public function test_rank_function_without_more_than_one_order_by_entries(): void
    {
        $this->expectExceptionMessage('Dens Rank window function supports only one order by column');

        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $densRank = dense_rank()->over(window()->orderBy(ref('salary'), ref('id')));

        static::assertSame(1, $densRank->apply($row1, $rows, flow_context()));
    }

    public function test_rank_function_without_order_by(): void
    {
        $this->expectExceptionMessage('Window function "dens_rank()" requires an OVER clause.');
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        dense_rank()->apply($row1, $rows, flow_context());
    }
}
