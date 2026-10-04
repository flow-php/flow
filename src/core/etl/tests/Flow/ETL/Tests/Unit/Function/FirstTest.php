<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class FirstTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], first(ref('value'))->references());
    }

    public function test_aggregation_firs_value(): void
    {
        $aggregator = first(ref('int'));

        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context(),
        );
        $aggregator->aggregate(array_to_rows([['int' => '10']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '20']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '55']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '25']], schema(str_schema('int'))), [0], flow_context());

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

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = first(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [1, 2, 3, 4]), schema(int_schema('v'))),
            [1, 3],
            flow_context(),
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
        $onePass = first(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = first(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = first(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        first(ref('v'))->merge(last(ref('v')), flow_context());
    }

    public function test_merge_into_an_empty_half_takes_the_other_value(): void
    {
        $rows = array_to_rows([['v' => 7]], schema(int_schema('v')));
        $right = first(ref('v'));
        $right->aggregate($rows, [0], flow_context());
        $left = first(ref('v'));

        $left->merge($right, flow_context());

        static::assertSame(7, $left->value());
    }
}
