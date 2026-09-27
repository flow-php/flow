<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\window;

final class CountTest extends FlowTestCase
{
    public function test_references_is_empty_without_a_reference(): void
    {
        static::assertSame([], count()->references());
    }

    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('id')], count(ref('id'))->references());
    }

    public function test_aggregation_count_from_numeric_values(): void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => '10'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '20'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '55'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '25'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'not_int' => null,
        ], schema(str_schema('not_int', nullable: true))), flow_context());

        static::assertSame(4, $aggregator->value());
    }

    public function test_aggregation_count_with_float_result(): void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10.25], schema(float_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 305], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 25], schema(int_schema('int'))), flow_context());

        static::assertSame(4, $aggregator->value());
    }

    public function test_aggregation_count_without_reference(): void
    {
        $aggregator = count();

        $aggregator->aggregate(array_to_row(['int' => '10'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '20'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '55'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '25'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'not_int' => null,
        ], schema(str_schema('not_int', nullable: true))), flow_context());

        static::assertSame(5, $aggregator->value());
    }

    public function test_aggregation_when_row_does_not_have_entry(): void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 30], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'int' => null,
        ], schema(int_schema('int', nullable: true))), flow_context());
        $aggregator->aggregate(array_to_row([
            'test' => null,
        ], schema(str_schema('test', nullable: true))), flow_context());

        static::assertSame(4, $aggregator->value());
    }

    public function test_window_function_count_of_a_reference_skips_nulls(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value', nullable: true)),
            $row1 = array_to_row(['id' => 1, 'value' => 10], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 2, 'value' => null], schema(int_schema('id'), int_schema('value', nullable: true))),
            array_to_row(['id' => 3, 'value' => 30], schema(int_schema('id'), int_schema('value'))),
        );

        $count = count(ref('value'))->over(window()->orderBy(ref('id')));

        static::assertSame(2, $count->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_count_without_a_reference_counts_every_row_in_the_frame(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value', nullable: true)),
            $row1 = array_to_row(['id' => 1, 'value' => 10], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 2, 'value' => null], schema(int_schema('id'), int_schema('value', nullable: true))),
            array_to_row(['id' => 3, 'value' => 30], schema(int_schema('id'), int_schema('value'))),
        );

        $count = count()->over(window()->orderBy(ref('id')));

        static::assertSame(3, $count->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_count_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Count window function error:');

        $rows = rows(
            schema(int_schema('id')),
            $row1 = array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        );

        $count = count(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $count->apply(WindowContextMother::forRow($row1, $rows, context: $context));
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = count(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_with_children_keeps_a_ref_less_count_a_leaf(): void
    {
        $aggregate = count();

        static::assertSame([], $aggregate->children());
        static::assertSame([], $aggregate->withChildren([])->references());
    }

    public function test_counting_nothing_is_zero(): void
    {
        static::assertSame(0, count(ref('int'))->value());
    }
}
