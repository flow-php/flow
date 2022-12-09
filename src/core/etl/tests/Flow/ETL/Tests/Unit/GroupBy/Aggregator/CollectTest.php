<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use function Flow\ETL\DSL\entry;
use function Flow\ETL\DSL\struct;
use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectTest extends TestCase
{
    public function test_collect_entry_values() : void
    {
        $aggregator = Aggregation::collect(entry('data'))->create();

        $aggregator->aggregate(Row::create(Entry::string('data', 'a')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'b')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'b')));
        $aggregator->aggregate(Row::create(Entry::string('data', 'c')));

        $this->assertSame(
            [
                'a', 'b', 'b', 'c',
            ],
            $aggregator->result()->value()
        );
    }

    public function test_collect_entry_values_as_structure() : void
    {
        $aggregator = Aggregation::collect(struct('a', 'b'))->create();

        $aggregator->aggregate(Row::create(Entry::string('a', 'z'), Entry::integer('b', 1)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'y'), Entry::integer('b', 5)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'u'), Entry::integer('b', 10)));
        $aggregator->aggregate(Row::create(Entry::string('a', 'z'), Entry::integer('b', 12)));

        $this->assertSame(
            [
                ['a' => 'z', 'b' => 1],
                ['a' => 'y', 'b' => 5],
                ['a' => 'u', 'b' => 10],
                ['a' => 'z', 'b' => 12],

            ],
            $aggregator->result()->value()
        );
    }
}
