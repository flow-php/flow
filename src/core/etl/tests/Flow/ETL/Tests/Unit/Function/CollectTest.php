<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectTest extends TestCase
{
    public function test_aggregation_collect_entry_values() : void
    {
        $aggregator = collect(ref('data'));

        $aggregator->aggregate(Row::create(str_entry('data', 'a')));
        $aggregator->aggregate(Row::create(str_entry('data', 'b')));
        $aggregator->aggregate(Row::create(str_entry('data', 'b')));
        $aggregator->aggregate(Row::create(str_entry('data', 'c')));

        $this->assertSame(
            [
                'a', 'b', 'b', 'c',
            ],
            $aggregator->result()->value()
        );
    }
}
