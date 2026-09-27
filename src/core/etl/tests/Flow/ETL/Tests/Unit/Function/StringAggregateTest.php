<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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

        $aggregator->aggregate(array_to_row(['data' => 'a'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'c'], schema(str_schema('data'))), flow_context());

        static::assertSame('a, b, b, c', $aggregator->value());
        static::assertSame('data_str_agg', $aggregator->outputName());
    }

    public function test_string_agg_on_empty_rows(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        static::assertSame('', $aggregator->value());
    }

    public function test_string_agg_on_non_string(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(array_to_row(['data' => 'a'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 1], schema(int_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'c'], schema(str_schema('data'))), flow_context());

        static::assertSame('c, b, a', $aggregator->value());
    }

    public function test_string_agg_with_alias(): void
    {
        $aggregator = string_agg(ref('data')->as('string'));

        $aggregator->aggregate(array_to_row(['data' => 'a'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'c'], schema(str_schema('data'))), flow_context());

        static::assertSame('a, b, b, c', $aggregator->value());
        static::assertSame('string', $aggregator->outputName());
    }

    public function test_string_agg_with_order(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(array_to_row(['data' => 'a'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'b'], schema(str_schema('data'))), flow_context());
        $aggregator->aggregate(array_to_row(['data' => 'c'], schema(str_schema('data'))), flow_context());

        static::assertSame('c, b, b, a', $aggregator->value());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = string_agg(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }
}
