<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_agg;

final class StringAggregateTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], string_agg(ref('value'), ',')->references());
    }

    public function test_string_agg(): void
    {
        $aggregator = string_agg(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame('a, b, b, c', $aggregator->result(flow_context(config())->entryFactory())->value());
        static::assertSame('data_str_agg', $aggregator->result(flow_context(config())->entryFactory())->name());
    }

    public function test_string_agg_on_empty_rows(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        static::assertSame('', $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_string_agg_on_non_string(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(int_entry('data', 1)), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame('c, b, a', $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_string_agg_with_alias(): void
    {
        $aggregator = string_agg(ref('data')->as('string'));

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame('a, b, b, c', $aggregator->result(flow_context(config())->entryFactory())->value());
        static::assertSame('string', $aggregator->result(flow_context(config())->entryFactory())->name());
    }

    public function test_string_agg_with_order(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame('c, b, b, a', $aggregator->result(flow_context(config())->entryFactory())->value());
    }
}
