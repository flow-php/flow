<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use PHPUnit\Framework\TestCase;

final class CountTest extends TestCase
{
    public function test_sum_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            $row2 = Row::create(Entry::int('id', 2), Entry::int('value', 1)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1)),
            Row::create(Entry::int('id', 1), Entry::int('value', 1)),
        );

        $window = Window::partitionBy(ref('value'))->orderBy(ref('id')->desc())->count(ref('id'));

        $this->assertSame(2, $window->function()->apply($row1, $rows, $window));
        $this->assertSame(1, $window->function()->apply($row2, $rows, $window));
    }
}
