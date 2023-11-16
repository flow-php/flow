<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\struct;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectTest extends TestCase
{
    public function test_aggregation_collect_entry_values() : void
    {
        $aggregator = collect(ref('data'));

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

    public function test_aggregation_collect_entry_values_as_structure() : void
    {
        $aggregator = collect(struct('a', 'b'));

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
