<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{collect, config, flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CollectTest extends FlowTestCase
{
    public function test_aggregation_collect_entry_values() : void
    {
        $aggregator = collect(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            [
                'a', 'b', 'b', 'c',
            ],
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }
}
