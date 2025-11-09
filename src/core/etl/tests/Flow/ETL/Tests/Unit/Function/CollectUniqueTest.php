<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{collect_unique, config, flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CollectUniqueTest extends FlowTestCase
{
    public function test_aggregation_collect_unique_values() : void
    {
        $aggregator = collect_unique(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        self::assertSame(
            [
                'a', 'b', 'c',
            ],
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }
}
