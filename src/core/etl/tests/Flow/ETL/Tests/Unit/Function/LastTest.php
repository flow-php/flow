<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{last, null_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class LastTest extends TestCase
{
    public function test_aggregation_last_value() : void
    {
        $aggregator = last(ref('int'));

        $aggregator->aggregate(Row::create(str_entry('int', '10')));
        $aggregator->aggregate(Row::create(str_entry('int', '20')));
        $aggregator->aggregate(Row::create(str_entry('int', '55')));
        $aggregator->aggregate(Row::create(str_entry('int', '25')));
        $aggregator->aggregate(Row::create(null_entry('not_int')));

        self::assertSame(
            '25',
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_last_value_when_nothing_aggregated() : void
    {
        $aggregator = last(ref('int'));

        self::assertEquals(
            new Row\Entry\NullEntry('int'),
            $aggregator->result()
        );
    }
}
