<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

final class CollectTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], collect(ref('value'))->references());
    }

    public function test_aggregation_collect_entry_values(): void
    {
        $aggregator = collect(ref('data'));

        $aggregator->aggregate(row(['data' => 'a']), flow_context());
        $aggregator->aggregate(row(['data' => 'b']), flow_context());
        $aggregator->aggregate(row(['data' => 'b']), flow_context());
        $aggregator->aggregate(row(['data' => 'c']), flow_context());

        static::assertSame(
            [
                'a',
                'b',
                'b',
                'c',
            ],
            $aggregator->value(),
        );
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = collect(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_collecting_nothing_yields_an_empty_list(): void
    {
        static::assertSame([], collect(ref('data'))->value());
    }

    public function test_a_nullable_source_column_yields_a_nullable_element_type(): void
    {
        $resolved = (new ReferenceResolver())->resolve(collect(ref('v')), schema(int_schema('v', true)));

        static::assertSame('?list<?integer>', $resolved->returns()->toString());
    }
}
