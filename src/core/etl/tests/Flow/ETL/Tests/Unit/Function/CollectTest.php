<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CollectTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], collect(ref('value'))->references());
    }

    public function test_aggregation_collect_entry_values(): void
    {
        $aggregator = collect(ref('data'));

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

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

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = collect(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
                'a',
                'b',
                'c',
                'd',
            ]), schema(str_schema('v'))),
            [1, 3],
            flow_context(),
        );

        static::assertSame(['b', 'd'], $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            'a',
            'b',
            'c',
            'd',
        ]), schema(str_schema('v')));
        $onePass = collect(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = collect(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = collect(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        collect(ref('v'))->merge(count(ref('v')), flow_context());
    }
}
