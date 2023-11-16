<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\ref;
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
}
