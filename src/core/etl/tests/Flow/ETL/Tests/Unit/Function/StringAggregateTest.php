<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, ref, str_entry, string_agg};
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
            $aggregator->result()->value()
        );
        self::assertSame('data_str_agg', $aggregator->result()->name());
    }

    public function test_string_agg_on_empty_rows() : void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        self::assertSame(
            '',
            $aggregator->result()->value()
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
            $aggregator->result()->value()
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
            $aggregator->result()->value()
        );
        self::assertSame('string', $aggregator->result()->name());
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
            $aggregator->result()->value()
        );
    }
}
