<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class CollectUniqueTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], collect_unique(ref('value'))->references());
    }

    public function test_aggregation_collect_unique_values(): void
    {
        $aggregator = collect_unique(ref('data'));

        $aggregator->aggregate(row(str_entry('data', 'a')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'b')), flow_context());
        $aggregator->aggregate(row(str_entry('data', 'c')), flow_context());

        static::assertSame(
            [
                'a',
                'b',
                'c',
            ],
            $aggregator->value(),
        );
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = collect_unique(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_collecting_nothing_yields_an_empty_list(): void
    {
        static::assertSame([], collect_unique(ref('data'))->value());
    }
}
