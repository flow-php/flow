<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{collect_unique, ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class CollectUniqueTest extends FlowTestCase
{
    public function test_aggregation_collect_unique_values() : void
    {
        $aggregator = collect_unique(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            [
                'a', 'b', 'c',
            ],
            $aggregator->result()->value()
        );
    }
}
