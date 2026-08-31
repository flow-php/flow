<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

final class FirstTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], first(ref('value'))->references());
    }

    public function test_aggregation_firs_value(): void
    {
        $aggregator = first(ref('int'));

        $aggregator->aggregate(row(['not_int' => null]), flow_context());
        $aggregator->aggregate(row(['int' => '10']), flow_context());
        $aggregator->aggregate(row(['int' => '20']), flow_context());
        $aggregator->aggregate(row(['int' => '55']), flow_context());
        $aggregator->aggregate(row(['int' => '25']), flow_context());

        static::assertSame('10', $aggregator->value());
    }

    public function test_aggregation_firs_value_when_nothing_aggregated(): void
    {
        $aggregator = first(ref('int'));

        static::assertNull($aggregator->value());
        static::assertSame('int_first', $aggregator->outputName());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = first(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_first_over_an_int_column_declares_optional_integer(): void
    {
        static::assertSame(
            '?integer',
            (new ReferenceResolver())
                ->resolve(first(ref('v')), schema(int_schema('v')))
                ->returns()
                ->toString(),
        );
    }
}
