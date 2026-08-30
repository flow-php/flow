<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_datetime;

final class MaxTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('int')], max(ref('int'))->references());
    }

    public function test_aggregation_max_from_numeric_values(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(['int' => '10']), flow_context());
        $aggregator->aggregate(row(['int' => '20']), flow_context());
        $aggregator->aggregate(row(['int' => '55']), flow_context());
        $aggregator->aggregate(row(['int' => '25']), flow_context());
        $aggregator->aggregate(row(['not_int' => null]), flow_context());

        static::assertSame(55.0, $aggregator->value());
    }

    public function test_aggregation_max_including_null_value(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30]), flow_context());
        $aggregator->aggregate(row(['int' => null]), flow_context());

        static::assertSame(30.0, $aggregator->value());
    }

    public function test_aggregation_max_with_datetime_values(): void
    {
        $aggregator = max(ref('datetime'));

        $aggregator->aggregate(row(['datetime' => type_datetime()->cast('2021-01-01 00:00:00')]), flow_context());
        $aggregator->aggregate(row(['datetime' => type_datetime()->cast('2021-01-02 00:00:00')]), flow_context());
        $aggregator->aggregate(row(['datetime' => type_datetime()->cast('2021-01-03 00:00:00')]), flow_context());
        $aggregator->aggregate(row(['datetime' => type_datetime()->cast('2021-01-04 00:00:00')]), flow_context());

        static::assertEquals(new DateTimeImmutable('2021-01-04 00:00:00'), $aggregator->value());
    }

    public function test_aggregation_max_with_float_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30.5]), flow_context());
        $aggregator->aggregate(row(['int' => 25]), flow_context());

        static::assertSame(30.5, $aggregator->value());
    }

    public function test_aggregation_max_with_integer_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30]), flow_context());
        $aggregator->aggregate(row(['int' => 40]), flow_context());

        static::assertSame(40.0, $aggregator->value());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = max(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_max_of_nothing_is_null(): void
    {
        static::assertNull(max(ref('int'))->value());
    }
}
