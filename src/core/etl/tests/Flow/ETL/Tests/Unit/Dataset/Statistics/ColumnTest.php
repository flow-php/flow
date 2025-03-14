<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Dataset\Statistics\Column;
use Flow\ETL\Tests\FlowTestCase;

final class ColumnTest extends FlowTestCase
{
    public function test_collecting_column_statistics() : void
    {
        $statistics = new Column(int_entry('a', 1));
        self::assertEquals(1, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(1, $statistics->max());
        self::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', 2));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(2, $statistics->max());
        self::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', null));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(1, $statistics->nullCount());
        self::assertEquals(2, $statistics->max());
        self::assertEquals(1, $statistics->min());
    }
}
