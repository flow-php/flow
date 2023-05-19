<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use PHPUnit\Framework\TestCase;

final class RankTest extends TestCase
{
    public function test_rank_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1)),
        );

        $window = Window::partitionBy(ref('value'))->orderBy(ref('id')->desc())->rank();

        $this->assertSame(5, $window->function()->apply($row1, $rows, $window));
    }
}
