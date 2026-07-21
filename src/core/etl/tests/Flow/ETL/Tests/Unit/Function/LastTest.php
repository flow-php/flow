<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;

final class LastTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], last(ref('value'))->references());
    }

    public function test_aggregation_last_value(): void
    {
        $aggregator = last(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        static::assertSame('25', $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_last_value_when_nothing_aggregated(): void
    {
        $aggregator = last(ref('int'));

        static::assertEquals(
            string_entry('int_last', null),
            $aggregator->result(flow_context(config())->entryFactory()),
        );
    }
}
