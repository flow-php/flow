<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregator\First;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class FirstTest extends TestCase
{
    public function test_firs_value() : void
    {
        $aggregator = new First('int');

        $aggregator->aggregate(Row::create(Entry::null('not_int')));
        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));

        $this->assertSame(
            '10',
            $aggregator->result()->value()
        );
    }

    public function test_firs_value_when_nothing_aggergated() : void
    {
        $aggregator = new First('int');

        $this->assertEquals(
            new Row\Entry\NullEntry('int'),
            $aggregator->result()
        );
    }
}
