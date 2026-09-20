<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Planner\PlannedNode;
use Flow\ETL\Planner\PlannedNodes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\LimitTransformer;

final class PlannedNodesTest extends FlowTestCase
{
    public function test_an_added_node_is_returned_by_identity(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::read();
        $plannedNode = new PlannedNode([], [], null);

        static::assertSame($plannedNode, $planned->add($node, $plannedNode));
        static::assertTrue($planned->has($node));
        static::assertSame($plannedNode, $planned->of($node));
        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_of_throws_for_a_node_never_planned(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('was never planned');

        (new PlannedNodes())->of(NodeMother::read());
    }

    public function test_it_keeps_the_first_refusal(): void
    {
        $planned = new PlannedNodes();
        $first = new SchemaNotDerivableException('first');

        static::assertNull($planned->refusal());

        $planned->refuse($first);
        $planned->refuse(new SchemaNotDerivableException('second'));

        static::assertSame($first, $planned->refusal());
    }

    public function test_steps_are_the_bound_ones_until_the_plan_refuses(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::read();
        $steps = [new LimitTransformer(5)];
        $bound = [new LimitTransformer(5)];
        $planned->add($node, new PlannedNode($steps, $bound, null));

        static::assertSame($bound, $planned->steps($node));

        $planned->refuse(new SchemaNotDerivableException('refused'));

        static::assertSame($steps, $planned->steps($node));
    }
}
