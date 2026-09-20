<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer\Rule;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\to_memory;

final class CombineSortAndLimitTest extends FlowTestCase
{
    public function test_a_limit_over_a_sort_becomes_a_top_n_over_the_sorts_input(): void
    {
        $read = NodeMother::read();
        $refs = refs(ref('id'));

        $plan = (new CombineSortAndLimit())->apply(
            NodeMother::plan(NodeMother::limit(new Sort($read, $refs), 5)),
            NodeMother::context(),
        );

        $topN = $plan->spine();
        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame([$read], $topN->children());
        static::assertSame($refs, $topN->refs);
        static::assertSame(5, $topN->limit);
    }

    public function test_a_limit_over_anything_else_is_left_alone(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::sort(NodeMother::read())), 5));

        static::assertSame($plan->root, (new CombineSortAndLimit())->apply($plan, NodeMother::context())->root);
    }

    public function test_the_rule_uses_the_context_it_is_given(): void
    {
        $context = NodeMother::context(config_builder()->sort(external_sort()->runSize(2))->build());
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::sort(NodeMother::read()), 3));

        static::assertInstanceOf(
            Limit::class,
            (new CombineSortAndLimit())
                ->apply($plan, $context)
                ->spine(),
        );
    }

    public function test_a_sort_another_consumer_reads_stays_for_that_consumer(): void
    {
        $sort = NodeMother::sort(NodeMother::read());
        $write = new Write($sort, to_memory(new ArrayMemory()));

        $plan = (new CombineSortAndLimit())->apply(
            new LogicalPlan(new Outputs(new Result(NodeMother::limit($sort, 5)), $write)),
            NodeMother::context(),
        );

        static::assertInstanceOf(TopN::class, $plan->spine());
        static::assertSame([$write], $plan->sinks()->all());
        static::assertSame($sort, $write->children()[0]);
    }
}
