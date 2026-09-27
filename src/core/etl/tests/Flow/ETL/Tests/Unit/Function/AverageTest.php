<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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

        $aggregator->aggregate(array_to_row(['int' => '10'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '20'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '30'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '25'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'not_int' => null,
        ], schema(str_schema('not_int', nullable: true))), flow_context());

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_including_null_value(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 30], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'int' => null,
        ], schema(int_schema('int', nullable: true))), flow_context());

        static::assertSame(20.0, $aggregator->value());
    }

    public function test_aggregation_average_with_float_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 30], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 25], schema(int_schema('int'))), flow_context());

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_with_integer_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 30], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 40], schema(int_schema('int'))), flow_context());

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
            $row1 = array_to_row(['id' => 1, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 2, 'value' => 100], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 3, 'value' => 25], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 4, 'value' => 64], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 5, 'value' => 23], schema(int_schema('id'), int_schema('value'))),
        );

        $avg = average(ref('value'))->over(window()->orderBy(ref('value')));

        static::assertSame(42.6, $avg->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_average_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Average window function error:');

        $rows = rows(
            schema(int_schema('id')),
            $row1 = array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        );

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
