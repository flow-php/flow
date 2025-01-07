<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, rank, ref, window};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class RankTest extends FlowTestCase
{
    public function test_rank_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)), $row2 = row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)), $row3 = row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)), $row4 = row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)), $row5 = row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)));

        $rank = rank()->over(window()->orderBy(ref('salary')->desc()));

        self::assertSame(1, $rank->apply($row1, $rows));
        self::assertSame(1, $rank->apply($row2, $rows));
        self::assertSame(1, $rank->apply($row3, $rows));
        self::assertSame(5, $rank->apply($row4, $rows));
        self::assertSame(4, $rank->apply($row5, $rows));
    }

    public function test_rank_function_without_more_than_one_order_by_entries() : void
    {
        $this->expectExceptionMessage('Rank window function supports only one order by column');

        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)), row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)));

        $rank = rank()->over(window()->partitionBy(ref('value'))->orderBy(ref('salary'), ref('id')));

        self::assertSame(1, $rank->apply($row1, $rows));
    }

    public function test_rank_function_without_order_by() : void
    {
        $this->expectExceptionMessage('Window function "rank()" requires an OVER clause.');
        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)), row(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)), row(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)));

        $rank = rank();

        self::assertSame(1, $rank->apply($row1, $rows));
    }
}
