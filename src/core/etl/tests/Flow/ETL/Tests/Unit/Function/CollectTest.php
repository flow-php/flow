<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class CollectTest extends FlowTestCase
{
    public function test_aggregation_collect_entry_values(): void
    {
        $aggregator = collect(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame(
            [
                'a',
                'b',
                'b',
                'c',
            ],
            $aggregator->result(flow_context(config())->entryFactory())->value(),
        );
    }
}
