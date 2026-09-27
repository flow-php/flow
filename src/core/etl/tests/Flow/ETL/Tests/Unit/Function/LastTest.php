<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class LastTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], last(ref('value'))->references());
    }

    public function test_aggregation_last_value(): void
    {
        $aggregator = last(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => '10'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '20'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '55'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '25'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'not_int' => null,
        ], schema(str_schema('not_int', nullable: true))), flow_context());

        static::assertSame('25', $aggregator->value());
    }

    public function test_aggregation_last_value_when_nothing_aggregated(): void
    {
        $aggregator = last(ref('int'));

        static::assertNull($aggregator->value());
        static::assertSame('int_last', $aggregator->outputName());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = last(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_last_over_an_int_column_declares_optional_integer(): void
    {
        static::assertSame(
            '?integer',
            (new ReferenceResolver())
                ->resolve(last(ref('v')), schema(int_schema('v')))
                ->returns()
                ->toString(),
        );
    }
}
