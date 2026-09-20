<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\JoinSides;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Join;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class JoinSidesTest extends FlowTestCase
{
    public function test_a_node_that_is_not_a_join_is_returned_as_it_is(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 5);

        static::assertSame($limit, (new JoinSides(Optimizer::default(), NodeMother::context()))->of($limit));
    }

    public function test_a_joins_right_side_is_optimized_as_its_own_plan(): void
    {
        $join = NodeMother::join(NodeMother::read(), NodeMother::plan(NodeMother::limit(NodeMother::read(), 3))->root);

        $rewritten = (new JoinSides(Optimizer::default(), NodeMother::context()))->of($join);

        static::assertInstanceOf(Join::class, $rewritten);
        static::assertSame(3, (new LogicalPlan($rewritten->right()))->source()->limit());
        static::assertSame($join->children()[0], $rewritten->children()[0]);
    }

    public function test_a_cross_joins_right_side_is_optimized_as_its_own_plan(): void
    {
        $join = NodeMother::crossJoin(
            NodeMother::read(),
            NodeMother::plan(NodeMother::limit(NodeMother::read(), 3))->root,
        );

        $rewritten = (new JoinSides(Optimizer::default(), NodeMother::context()))->of($join);

        static::assertInstanceOf(CrossJoin::class, $rewritten);
        static::assertSame(3, (new LogicalPlan($rewritten->right()))->source()->limit());
    }

    public function test_a_join_whose_right_side_does_not_change_keeps_its_identity(): void
    {
        $join = NodeMother::crossJoin(NodeMother::read(), NodeMother::plan(NodeMother::read())->root);

        static::assertSame($join, (new JoinSides(new Optimizer(), NodeMother::context()))->of($join));
    }
}
