<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{dense_rank, int_entry, ref, window};
use Flow\ETL\{Row, Rows};
use PHPUnit\Framework\TestCase;

final class DenseRankTest extends TestCase
{
    public function test_rank_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            $row2 = Row::create(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            $row3 = Row::create(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            $row4 = Row::create(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            $row5 = Row::create(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $denseRank = dense_rank()->over(window()->orderBy(ref('salary')->desc()));

        self::assertSame(1, $denseRank->apply($row1, $rows));
        self::assertSame(1, $denseRank->apply($row2, $rows));
        self::assertSame(1, $denseRank->apply($row3, $rows));
        self::assertSame(3, $denseRank->apply($row4, $rows));
        self::assertSame(2, $denseRank->apply($row5, $rows));
    }

    public function test_rank_function_without_more_than_one_order_by_entries() : void
    {
        $this->expectExceptionMessage('Dens Rank window function supports only one order by column');

        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            Row::create(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $densRank = dense_rank()->over(window()->orderBy(ref('salary'), ref('id')));

        self::assertSame(1, $densRank->apply($row1, $rows));
    }

    public function test_rank_function_without_order_by() : void
    {
        $this->expectExceptionMessage('Window function "dens_rank()" requires an OVER clause.');
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            Row::create(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        dense_rank()->apply($row1, $rows);
    }
}
