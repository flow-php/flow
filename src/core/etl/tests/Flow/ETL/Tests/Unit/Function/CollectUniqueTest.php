<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollectUniqueTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], collect_unique(ref('value'))->references());
    }

    public function test_aggregation_collect_unique_values(): void
    {
        $aggregator = collect_unique(ref('data'));

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

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

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = collect_unique(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
                'a',
                'b',
                'a',
                'c',
            ]), schema(str_schema('v'))),
            [0, 2],
            flow_context(),
        );

        static::assertSame(['a'], $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            'a',
            'b',
            'a',
            'c',
        ]), schema(str_schema('v')));
        $onePass = collect_unique(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = collect_unique(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = collect_unique(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        collect_unique(ref('v'))->merge(collect(ref('v')), flow_context());
    }
}
