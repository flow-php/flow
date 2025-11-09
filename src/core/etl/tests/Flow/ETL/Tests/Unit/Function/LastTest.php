<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, last, ref, str_entry, string_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class LastTest extends FlowTestCase
{
    public function test_aggregation_last_value() : void
    {
        $aggregator = last(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        self::assertSame(
            '25',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_last_value_when_nothing_aggregated() : void
    {
        $aggregator = last(ref('int'));

        self::assertEquals(
            string_entry('int_last', null),
            $aggregator->result(flow_context(config())->entryFactory())
        );
    }
}
