<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\dens_rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
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

        $densRank = dens_rank()->over(window()->orderBy(ref('salary')->desc()));

        $this->assertSame(1, $densRank->apply($row1, $rows));
        $this->assertSame(1, $densRank->apply($row2, $rows));
        $this->assertSame(1, $densRank->apply($row3, $rows));
        $this->assertSame(3, $densRank->apply($row4, $rows));
        $this->assertSame(2, $densRank->apply($row5, $rows));
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

        $densRank = dens_rank()->over(window()->orderBy(ref('salary'), ref('id')));

        $this->assertSame(1, $densRank->apply($row1, $rows));
    }

    public function test_rank_function_without_order_by() : void
    {
        $this->expectExceptionMessage('Window function "dens_rank()" requires an OVER clause.');
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1), Entry::int('salary', 6000)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1), Entry::int('salary', 2000)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1), Entry::int('salary', 4000)),
        );

        dens_rank()->apply($row1, $rows);
    }
}
