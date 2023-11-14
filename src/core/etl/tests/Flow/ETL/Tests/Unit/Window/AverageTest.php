<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use PHPUnit\Framework\TestCase;

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

        $window = Window::partitionBy(ref('value'))->orderBy(ref('id')->desc())->avg(ref('value'));

        $this->assertSame(42.6, $window->function()->apply($row1, $rows, $window));
    }
}
