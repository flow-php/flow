<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RankTest extends TestCase
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

        $rank = rank()->over(window()->orderBy(ref('salary')->desc()));

        $this->assertSame(1, $rank->apply($row1, $rows));
        $this->assertSame(1, $rank->apply($row2, $rows));
        $this->assertSame(1, $rank->apply($row3, $rows));
        $this->assertSame(5, $rank->apply($row4, $rows));
        $this->assertSame(4, $rank->apply($row5, $rows));
    }

    public function test_rank_function_without_more_than_one_order_by_entries() : void
    {
        $this->expectExceptionMessage('Rank window function supports only one order by column');

        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            Row::create(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $rank = rank()->over(window()->partitionBy(ref('value'))->orderBy(ref('salary'), ref('id')));

        $this->assertSame(1, $rank->apply($row1, $rows));
    }

    public function test_rank_function_without_order_by() : void
    {
        $this->expectExceptionMessage('Window function "rank()" requires an OVER clause.');
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 2), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 3), int_entry('value', 1), int_entry('salary', 6000)),
            Row::create(int_entry('id', 4), int_entry('value', 1), int_entry('salary', 2000)),
            Row::create(int_entry('id', 5), int_entry('value', 1), int_entry('salary', 4000)),
        );

        $rank = rank();

        $this->assertSame(1, $rank->apply($row1, $rows));
    }
}
