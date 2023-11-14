<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use function Flow\ETL\DSL\ref;
use Flow\ETL\_Window;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class DensRankTest extends TestCase
{
    public function test_rank_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1), Entry::int('salary', 6000)),
            $row2 = Row::create(Entry::int('id', 2), Entry::int('value', 1), Entry::int('salary', 6000)),
            $row3 = Row::create(Entry::int('id', 3), Entry::int('value', 1), Entry::int('salary', 6000)),
            $row4 = Row::create(Entry::int('id', 4), Entry::int('value', 1), Entry::int('salary', 2000)),
            $row5 = Row::create(Entry::int('id', 5), Entry::int('value', 1), Entry::int('salary', 4000)),
        );

        $window = _Window::partitionBy(ref('value'))->orderBy(ref('salary')->desc())->densRank();

        $this->assertSame(1, $window->function()->apply($row1, $rows, $window));
        $this->assertSame(1, $window->function()->apply($row2, $rows, $window));
        $this->assertSame(1, $window->function()->apply($row3, $rows, $window));
        $this->assertSame(3, $window->function()->apply($row4, $rows, $window));
        $this->assertSame(2, $window->function()->apply($row5, $rows, $window));
    }

    public function test_rank_function_without_more_than_one_order_by_entries() : void
    {
        $this->expectExceptionMessage('Dens Rank window function supports only one order by column');

        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1), Entry::int('salary', 2000)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1), Entry::int('salary', 4000)),
        );

        $window = _Window::partitionBy(ref('value'))->orderBy(ref('salary'), ref('id'))->densRank();

        $this->assertSame(1, $window->function()->apply($row1, $rows, $window));
    }

    public function test_rank_function_without_order_by() : void
    {
        $this->expectExceptionMessage('Dens Rank window function requires to be ordered by one column');
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1), Entry::int('salary', 2000)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1), Entry::int('salary', 4000)),
        );

        $window = _Window::partitionBy(ref('value'))->densRank();

        $this->assertSame(1, $window->function()->apply($row1, $rows, $window));
    }
}
