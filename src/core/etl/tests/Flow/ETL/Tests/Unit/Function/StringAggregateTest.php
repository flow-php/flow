<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
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

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

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

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 1]], schema(int_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

        static::assertSame('c, b, a', $aggregator->value());
    }

    public function test_string_agg_with_alias(): void
    {
        $aggregator = string_agg(ref('data')->as('string'));

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

        static::assertSame('a, b, b, c', $aggregator->value());
        static::assertSame('string', $aggregator->outputName());
    }

    public function test_string_agg_with_order(): void
    {
        $aggregator = string_agg(ref('data'), sort: SortOrder::DESC);

        $aggregator->aggregate(array_to_rows([['data' => 'a']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'b']], schema(str_schema('data'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['data' => 'c']], schema(str_schema('data'))), [0], flow_context());

        static::assertSame('c, b, b, a', $aggregator->value());
    }

    #[TestWith([SortOrder::ASC, '10,100,9,B'])]
    #[TestWith([SortOrder::DESC, 'B,9,100,10'])]
    public function test_string_agg_orders_numeric_looking_strings_by_bytes(SortOrder $order, string $expected): void
    {
        $aggregator = string_agg(ref('data'), ',', $order);
        $aggregator->aggregate(
            array_to_rows([
                ['data' => '9'],
                ['data' => '10'],
                ['data' => '100'],
                ['data' => 'B'],
            ], schema(str_schema('data'))),
            [0, 1, 2, 3],
            flow_context(),
        );

        static::assertSame($expected, $aggregator->value());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = string_agg(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = string_agg(ref('v'));
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

        static::assertSame('b, d', $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            'a',
            'b',
            'c',
            'd',
        ]), schema(str_schema('v')));
        $onePass = string_agg(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = string_agg(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = string_agg(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        string_agg(ref('v'))->merge(collect(ref('v')), flow_context());
    }
}
