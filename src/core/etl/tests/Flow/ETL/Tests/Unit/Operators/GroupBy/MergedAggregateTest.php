<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Functions\First;
use Flow\ETL\Functions\Sum;
use Flow\ETL\Operators\GroupBy\MergedAggregate;
use Flow\ETL\Tests\Context\AggregatorFixtures;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_is_nullable;

final class MergedAggregateTest extends FlowTestCase
{
    public function test_merges_the_partial_column_in_row_order(): void
    {
        $inner = AggregatorFixtures::resolved(First::class);
        $partials = array_to_rows([
            ['v_first' => ['value' => 3, 'found' => true]],
            ['v_first' => ['value' => 7, 'found' => true]],
        ], schema(structure_schema('v_first', $inner->partialType())));

        $merged = new MergedAggregate($inner);
        $merged->aggregate($partials, [0, 1], flow_context()->backend());

        static::assertSame(3, $merged->value());
        static::assertSame($inner->returns()->toString(), $merged->returns()->toString());
        static::assertEquals([ref('v_first')], $merged->references());
    }

    public function test_declares_the_inner_nullable_type_a_column_can_carry(): void
    {
        $returns = (new MergedAggregate(AggregatorFixtures::resolved(Sum::class)))->returns();

        static::assertTrue(type_is_nullable($returns));
        static::assertTrue(definition_from_type('out', $returns)->isNullable());
    }

    public function test_has_no_children(): void
    {
        static::assertSame([], (new MergedAggregate(AggregatorFixtures::resolved(Sum::class)))->children());
    }

    public function test_with_children_is_fresh(): void
    {
        $inner = AggregatorFixtures::resolved(Sum::class);
        $inner->aggregate(AggregatorFixtures::rows(new PhpBackend()), [0, 1], flow_context()->backend());
        $merged = new MergedAggregate($inner);

        $fresh = $merged->withChildren([]);

        static::assertNotSame($merged, $fresh);
        static::assertSame(4.0, $merged->value());
        static::assertNull($fresh->value());
        static::assertSame('v_sum', $fresh->outputName());
    }

    public function test_merge_unwraps_another_merged_aggregate(): void
    {
        $inner = AggregatorFixtures::resolved(Sum::class);
        $partials = array_to_rows([
            ['v_sum' => ['aggregated' => 1, 'sum' => ['int' => 3, 'float' => null, 'compensation' => 0.0]]],
            ['v_sum' => ['aggregated' => 1, 'sum' => ['int' => 7, 'float' => null, 'compensation' => 0.0]]],
        ], schema(structure_schema('v_sum', $inner->partialType())));
        $left = new MergedAggregate($inner);
        $left->aggregate($partials, [0], flow_context()->backend());
        $right = new MergedAggregate(AggregatorFixtures::resolved(Sum::class));
        $right->aggregate($partials, [1], flow_context()->backend());

        $left->merge($right->partial(), flow_context()->backend());

        static::assertSame(10.0, $left->value());
    }

    public function test_describe_round_trips(): void
    {
        $merged = new MergedAggregate(AggregatorFixtures::resolved(Sum::class));

        static::assertSame(
            $merged->partialType()->toString(),
            DescribeRoundTrip::rebuilt($merged)->partialType()->toString(),
        );
    }
}
