<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectUniqueTest extends TestCase
{
    public function test_sum_from_numeric_values() : void
    {
        $aggregator = Aggregation::collect_unique('data')->create();

        $aggregator->aggregate(Row::create(Entry::string('data', 'a')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'b')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'b')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'c')));

        $this->assertSame(
            [
                'a', 'b', 'c',
            ],
            $aggregator->result()->value()
        );
    }
}
