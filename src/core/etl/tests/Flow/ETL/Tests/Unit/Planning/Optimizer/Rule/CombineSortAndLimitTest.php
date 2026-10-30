<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Values\Memory\Unit;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class CombineSortAndLimitTest extends FlowTestCase
{
    public function test_a_limit_over_a_sort_becomes_a_top_n_over_the_sorts_input(): void
    {
        $read = NodeMother::read();
        $columns = [ref('id')->asc()];

        $topN = (new CombineSortAndLimit())
            ->apply(NodeMother::plan(NodeMother::limit(new Sort($read, $columns), 5)), config()->backend())
            ->spine();
        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame([$read], $topN->children());
        static::assertSame($columns, $topN->columns);
        static::assertSame(5, $topN->limit);
    }

    public function test_a_limit_over_anything_else_is_left_alone(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::sort(NodeMother::read())), 5));

        static::assertSame($plan->root, (new CombineSortAndLimit())->apply($plan, config()->backend())->root);
    }

    public function test_a_sort_another_consumer_reads_stays_for_that_consumer(): void
    {
        $sort = NodeMother::sort(NodeMother::read());
        $write = new Write($sort, to_memory(new ArrayMemory()));

        $plan = (new CombineSortAndLimit())->apply(
            new LogicalPlan(new Outputs(new Result(NodeMother::limit($sort, 5)), $write)),
            config()->backend(),
        );

        static::assertInstanceOf(TopN::class, $plan->spine());
        static::assertSame([$write], $plan->writes()->all());
        static::assertSame($sort, $write->children()[0]);
    }

    public function test_a_node_that_is_not_a_limit_is_returned_as_is(): void
    {
        $sort = NodeMother::sort(NodeMother::read());

        static::assertSame($sort, (new CombineSortAndLimit())->of($sort, $sort));
    }

    public function test_a_limit_over_anything_but_a_sort_is_returned_as_is(): void
    {
        $limit = NodeMother::limit(NodeMother::select(NodeMother::sort(NodeMother::read())), 5);

        static::assertSame($limit, (new CombineSortAndLimit())->of($limit, $limit));
    }

    public function test_a_limit_over_a_sort_becomes_a_top_n_over_the_sorts_input_columns_and_limit(): void
    {
        $read = NodeMother::read();
        $columns = [ref('id')->asc()];

        $limit = NodeMother::limit(new Sort($read, $columns), 5);
        $topN = (new CombineSortAndLimit())->of($limit, $limit);

        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame([$read], $topN->children());
        static::assertSame($columns, $topN->columns);
        static::assertSame(5, $topN->limit);
    }

    public function test_a_limit_over_any_sort_is_rewritten_and_keeps_the_pinned_algorithm(): void
    {
        $algorithm = external_sort()->memoryLimit(Unit::fromMb(1));

        $limit = NodeMother::limit(new Sort(NodeMother::read(), [ref('id')->asc()], $algorithm), 1_000_000);
        $topN = (new CombineSortAndLimit())->of($limit, $limit);

        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame($algorithm, $topN->algorithm);
    }
}
