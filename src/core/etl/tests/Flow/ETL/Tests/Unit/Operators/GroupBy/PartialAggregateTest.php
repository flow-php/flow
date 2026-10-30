<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Functions\Sum;
use Flow\ETL\Operators\GroupBy\PartialAggregate;
use Flow\ETL\Tests\Context\AggregatorFixtures;
use Flow\ETL\Tests\Context\AggregatorRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\Types\DSL\type_is_nullable;

final class PartialAggregateTest extends FlowTestCase
{
    public function test_value_is_the_partial(): void
    {
        $inner = AggregatorFixtures::resolved(Sum::class);
        $partial = new PartialAggregate($inner);

        $partial->aggregate(AggregatorFixtures::rows(new PhpBackend()), [0, 1], flow_context()->backend());

        static::assertSame($inner->partial(), $partial->value());
        static::assertSame(
            ['aggregated' => 2, 'sum' => ['int' => 4, 'float' => null, 'compensation' => 0.0]],
            $partial->value(),
        );
        static::assertSame('v_sum', $partial->outputName());
    }

    public function test_returns_the_partial_type(): void
    {
        $inner = AggregatorFixtures::resolved(Sum::class);

        static::assertSame($inner->partialType()->toString(), (new PartialAggregate($inner))->returns()->toString());
    }

    public function test_merge_unwraps_another_partial_aggregate(): void
    {
        $rows = AggregatorFixtures::rows(new PhpBackend());
        $left = new PartialAggregate(AggregatorFixtures::resolved(Sum::class));
        $left->aggregate($rows, [0, 1], flow_context()->backend());
        $right = new PartialAggregate(AggregatorFixtures::resolved(Sum::class));
        $right->aggregate($rows, [3, 4], flow_context()->backend());

        $left->merge($right->partial(), flow_context()->backend());

        static::assertSame(
            ['aggregated' => 4, 'sum' => ['int' => 14, 'float' => null, 'compensation' => 0.0]],
            $left->value(),
        );
    }

    public function test_describe_round_trips(): void
    {
        [$original, $copy] = AggregatorRoundTrip::aggregated(new PartialAggregate(sum(ref('v'))));

        static::assertSame($original, $copy);
    }

    public function test_declares_a_not_null_structure_a_column_can_carry(): void
    {
        $returns = (new PartialAggregate(AggregatorFixtures::resolved(Sum::class)))->returns();

        static::assertFalse(type_is_nullable($returns));
        static::assertFalse(definition_from_type('out', $returns)->isNullable());
    }

    public function test_with_children_rebuilds_the_inner_aggregate(): void
    {
        $rebuilt = (new PartialAggregate(sum(ref('v'))))->withChildren([ref('w')]);

        static::assertEquals([ref('w')], $rebuilt->children());
        static::assertSame('w_sum', $rebuilt->outputName());
    }
}
