<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{collect_unique, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CollectUniqueTest extends TestCase
{
    public function test_aggregation_collect_unique_values() : void
    {
        $aggregator = collect_unique(ref('data'));

        $aggregator->aggregate(Row::create(str_entry('data', 'a')));
        $aggregator->aggregate(Row::create(str_entry('data', 'b')));
        $aggregator->aggregate(Row::create(str_entry('data', 'b')));
        $aggregator->aggregate(Row::create(str_entry('data', 'c')));

        $this->assertSame(
            [
                'a', 'b', 'c',
            ],
            $aggregator->result()->value()
        );
    }
}
