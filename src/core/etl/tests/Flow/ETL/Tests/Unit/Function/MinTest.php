<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class MinTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('int')], min(ref('int'))->references());
    }

    public function test_the_output_suffix_survives_an_immutable_reference(): void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());

        static::assertSame('int_min', $aggregator->outputName());
    }

    public function test_aggregation_min_from_numeric_values(): void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        static::assertSame(10.0, $aggregator->value());
    }

    public function test_aggregation_min_including_null_value(): void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(str_entry('int', null)), flow_context());

        static::assertSame(10.0, $aggregator->value());
    }

    public function test_aggregation_min_with_datetime_values(): void
    {
        $aggregator = min(ref('datetime'));

        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-01 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-02 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-03 00:00:00')), flow_context());
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-04 00:00:00')), flow_context());

        static::assertEquals(new DateTimeImmutable('2021-01-01 00:00:00'), $aggregator->value());
    }

    public function test_aggregation_min_with_float_result(): void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 305)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        static::assertSame(10.25, $aggregator->value());
    }

    public function test_aggregation_min_with_integer_result(): void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 40)), flow_context());

        static::assertSame(10.0, $aggregator->value());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = min(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_min_of_nothing_is_null(): void
    {
        static::assertNull(min(ref('int'))->value());
    }
}
