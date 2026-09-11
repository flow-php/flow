<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class AverageTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('int')], average(ref('int'))->references());
    }

    public function test_aggregation_average_from_numeric_values(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(['int' => '10']), flow_context());
        $aggregator->aggregate(row(['int' => '20']), flow_context());
        $aggregator->aggregate(row(['int' => '30']), flow_context());
        $aggregator->aggregate(row(['int' => '25']), flow_context());
        $aggregator->aggregate(row(['not_int' => null]), flow_context());

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_including_null_value(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30]), flow_context());
        $aggregator->aggregate(row(['int' => null]), flow_context());

        static::assertSame(20.0, $aggregator->value());
    }

    public function test_aggregation_average_with_float_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30]), flow_context());
        $aggregator->aggregate(row(['int' => 25]), flow_context());

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_with_integer_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(['int' => 10]), flow_context());
        $aggregator->aggregate(row(['int' => 20]), flow_context());
        $aggregator->aggregate(row(['int' => 30]), flow_context());
        $aggregator->aggregate(row(['int' => 40]), flow_context());

        static::assertSame(25.0, $aggregator->value());
    }

    public function test_aggregation_average_of_nothing_is_null(): void
    {
        $aggregator = average(ref('int'));

        static::assertNull($aggregator->value());
    }

    public function test_window_function_average_on_partitioned_rows(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value')),
            $row1 = row(['id' => 1, 'value' => 1]),
            row(['id' => 2, 'value' => 100]),
            row(['id' => 3, 'value' => 25]),
            row(['id' => 4, 'value' => 64]),
            row(['id' => 5, 'value' => 23]),
        );

        $avg = average(ref('value'))->over(window()->orderBy(ref('value')));

        static::assertSame(42.6, $avg->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_average_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Average window function error:');

        $rows = rows(schema(int_schema('id')), $row1 = row(['id' => 1]), row(['id' => 2]));

        $avg = average(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $avg->apply(WindowContextMother::forRow($row1, $rows, context: $context));
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = average(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }
}
