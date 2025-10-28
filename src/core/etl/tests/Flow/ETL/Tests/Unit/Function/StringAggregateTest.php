<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, int_entry, ref, str_entry, string_agg};
use function Flow\ETL\DSL\row;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;

final class StringAggregateTest extends FlowTestCase
{
    public function test_string_agg() : void
    {
        $aggregator = string_agg(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            'a, b, b, c',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
        self::assertSame(
            'data_str_agg',
            $aggregator->result(flow_context(config())->entryFactory())->name()
        );
    }

    public function test_string_agg_on_empty_rows() : void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        self::assertSame(
            '',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_string_agg_on_non_string() : void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(int_entry('data', 1)));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            'c, b, a',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_string_agg_with_alias() : void
    {
        $aggregator = string_agg(ref('data')->as('string'));

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            'a, b, b, c',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
        self::assertSame(
            'string',
            $aggregator->result(flow_context(config())->entryFactory())->name()
        );
    }

    public function test_string_agg_with_order() : void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(row(str_entry('data', 'a')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'b')));
        $aggregator->aggregate(row(str_entry('data', 'c')));

        self::assertSame(
            'c, b, b, a',
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }
}
