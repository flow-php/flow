<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use function Flow\ETL\DSL\entry;
use function Flow\ETL\DSL\struct;
use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectUniqueTest extends TestCase
{
    public function test_collect_unique_values() : void
    {
        $aggregator = Aggregation::collect_unique(entry('data'))->create();

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

    public function test_collect_unique_values_entry_values_as_structure() : void
    {
        $aggregator = Aggregation::collect_unique(struct('a', 'b'))->create();

        $aggregator->aggregate(Row::create(Entry::string('a', 'z'), Entry::integer('b', 1)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'y'), Entry::integer('b', 5)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'u'), Entry::integer('b', 10)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'z'), Entry::integer('b', 1)));

        $this->assertSame(
            [
                ['a' => 'z', 'b' => 1],
                ['a' => 'y', 'b' => 5],
                ['a' => 'u', 'b' => 10],

            ],
            $aggregator->result()->value()
        );
    }
}
