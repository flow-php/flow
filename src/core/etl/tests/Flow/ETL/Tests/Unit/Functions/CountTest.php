<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Tests\Context\AggregatorRoundTrip;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\window;

final class CountTest extends FlowTestCase
{
    public function test_a_count_without_a_reference_describes_a_null_reference(): void
    {
        static::assertNull(count()->describe()?->data['ref']);
        static::assertNull(DescribeRoundTrip::rebuilt(count())->references()[0] ?? null);
    }

    public function test_with_no_children_is_a_fresh_instance(): void
    {
        $aggregate = count();
        $aggregate->aggregate(
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a'))),
            [0, 1],
            flow_context(config())->backend(),
        );

        $fresh = $aggregate->withChildren([]);

        static::assertNotSame($aggregate, $fresh);
        static::assertSame(2, $aggregate->value());
        static::assertSame(0, $fresh->value());
    }

    public function test_describe_round_trips(): void
    {
        [$original, $copy] = AggregatorRoundTrip::aggregated(count(ref('v')));

        static::assertSame($original, $copy);
    }

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

        $aggregator->aggregate(
            array_to_rows([['int' => '10']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '20']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '55']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '25']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context()->backend(),
        );

        static::assertSame(4, $aggregator->value());
    }

    public function test_aggregation_count_with_float_result(): void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(
            array_to_rows([['int' => 10.25]], schema(float_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => 20]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => 305]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => 25]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );

        static::assertSame(4, $aggregator->value());
    }

    public function test_aggregation_count_without_reference(): void
    {
        $aggregator = count();

        $aggregator->aggregate(
            array_to_rows([['int' => '10']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '20']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '55']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => '25']], schema(str_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context()->backend(),
        );

        static::assertSame(5, $aggregator->value());
    }

    public function test_aggregation_when_row_does_not_have_entry(): void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(
            array_to_rows([['int' => 10]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => 20]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([['int' => 30]], schema(int_schema('int'))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'int' => null,
            ]], schema(int_schema('int', nullable: true))),
            [0],
            flow_context()->backend(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'test' => null,
            ]], schema(str_schema('test', nullable: true))),
            [0],
            flow_context()->backend(),
        );

        static::assertSame(3, $aggregator->value());
    }

    public function test_window_function_count_of_a_reference_skips_nulls(): void
    {
        static::assertSame(
            2,
            count(ref('value'))
                ->over(window())
                ->values(
                    array_to_rows(
                        [['id' => 1, 'value' => 10], ['id' => 2, 'value' => null], ['id' => 3, 'value' => 30]],
                        schema(int_schema('id'), int_schema('value', nullable: true)),
                    ),
                    flow_context()->backend(),
                )[0],
        );
    }

    public function test_window_function_count_without_a_reference_counts_every_row_in_the_frame(): void
    {
        static::assertSame(
            3,
            count()
                ->over(window())
                ->values(
                    array_to_rows(
                        [['id' => 1, 'value' => 10], ['id' => 2, 'value' => null], ['id' => 3, 'value' => 30]],
                        schema(int_schema('id'), int_schema('value', nullable: true)),
                    ),
                    flow_context()->backend(),
                )[0],
        );
    }

    public function test_window_function_count_of_a_missing_reference_is_zero(): void
    {
        static::assertSame(
            [0, 0],
            count(ref('missing_column'))
                ->over(window())
                ->values(array_to_rows([
                    ['id' => 1],
                    ['id' => 2],
                ], schema(int_schema('id'))), flow_context()->backend()),
        );
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

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = count(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [1, 2, 3, 4]), schema(int_schema('v'))),
            [1, 3],
            flow_context()->backend(),
        );

        static::assertSame(2, $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            1,
            2,
            3,
            4,
        ]), schema(int_schema('v')));
        $onePass = count(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context()->backend());
        $left = count(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context()->backend());
        $right = count(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context()->backend());

        $left->merge($right->partial(), flow_context()->backend());

        static::assertSame($onePass->value(), $left->value());
    }
}
