<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Join;
use Flow\ETL\Planning\Optimizer\JoinSides;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config;

final class JoinSidesTest extends FlowTestCase
{
    public function test_a_node_that_is_not_a_join_is_returned_as_it_is(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 5);

        static::assertSame($limit, (new JoinSides(Optimizer::default(), config()->backend()))->of($limit, $limit));
    }

    public function test_a_joins_right_side_is_optimized_as_its_own_plan(): void
    {
        $join = NodeMother::join(NodeMother::read(), NodeMother::plan(NodeMother::limit(NodeMother::read(), 3))->root);

        $rewritten = (new JoinSides(Optimizer::default(), config()->backend()))->of($join, $join);

        static::assertInstanceOf(Join::class, $rewritten);
        static::assertSame(3, (new LogicalPlan($rewritten->right()))->source()->criteria()->limit);
        static::assertSame($join->children()[0], $rewritten->children()[0]);
    }

    public function test_a_cross_joins_right_side_is_optimized_as_its_own_plan(): void
    {
        $join = NodeMother::crossJoin(
            NodeMother::read(),
            NodeMother::plan(NodeMother::limit(NodeMother::read(), 3))->root,
        );

        $rewritten = (new JoinSides(Optimizer::default(), config()->backend()))->of($join, $join);

        static::assertInstanceOf(CrossJoin::class, $rewritten);
        static::assertSame(3, (new LogicalPlan($rewritten->right()))->source()->criteria()->limit);
    }

    public function test_a_join_whose_right_side_does_not_change_keeps_its_identity(): void
    {
        $join = NodeMother::crossJoin(NodeMother::read(), NodeMother::plan(NodeMother::read())->root);

        static::assertSame($join, (new JoinSides(new Optimizer(), config()->backend()))->of($join, $join));
    }
}
