<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregator\Last;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class LastTest extends TestCase
{
    public function test_last_value() : void
    {
        $aggregator = new Last('int');

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            '25',
            $aggregator->result()->value()
        );
    }

    public function test_last_value_when_nothing_aggregated() : void
    {
        $aggregator = new Last('int');

        $this->assertEquals(
            new Row\Entry\NullEntry('int'),
            $aggregator->result()
        );
    }
}
