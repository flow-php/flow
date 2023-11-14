<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;
use function Flow\ETL\DSL\window;

final class AverageTest extends TestCase
{
    public function test_average_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            Row::create(Entry::int('id', 2), Entry::int('value', 100)),
            Row::create(Entry::int('id', 3), Entry::int('value', 25)),
            Row::create(Entry::int('id', 4), Entry::int('value', 64)),
            Row::create(Entry::int('id', 5), Entry::int('value', 23)),
        );

        $expression = average(ref('value'))->over(window()->orderBy(ref('value')));

        $this->assertSame(42.6, $expression->function()->apply($row1, $rows, $expression->window()));
    }
}
