<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class MaxTest extends FlowTestCase
{
    public function test_aggregation_max_from_numeric_values(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        static::assertSame(55, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_max_including_null_value(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(str_entry('int', null)), flow_context());

        static::assertSame(30, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_max_with_datetime_values(): void
    {
        $aggregator = max(ref('datetime'));

        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-01 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-02 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-03 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-04 00:00:00')), flow_context());

        static::assertEquals(
            new DateTimeImmutable('2021-01-04 00:00:00'),
            $aggregator->result(flow_context(config())->entryFactory())->value(),
        );
    }

    public function test_aggregation_max_with_float_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(float_entry('int', 30.5)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        static::assertSame(30.5, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_max_with_integer_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 40)), flow_context());

        static::assertSame(40, $aggregator->result(flow_context(config())->entryFactory())->value());
    }
}
