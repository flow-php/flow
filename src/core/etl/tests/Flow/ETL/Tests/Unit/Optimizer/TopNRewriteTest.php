<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Optimizer\TopNRewrite;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class TopNRewriteTest extends FlowTestCase
{
    public function test_a_node_that_is_not_a_limit_is_returned_as_is(): void
    {
        $sort = NodeMother::sort(NodeMother::read());

        static::assertSame($sort, (new TopNRewrite(NodeMother::context()))->of($sort));
    }

    public function test_a_limit_over_anything_but_a_sort_is_returned_as_is(): void
    {
        $limit = NodeMother::limit(NodeMother::select(NodeMother::sort(NodeMother::read())), 5);

        static::assertSame($limit, (new TopNRewrite(NodeMother::context()))->of($limit));
    }

    public function test_a_limit_over_a_sort_becomes_a_top_n_over_the_sorts_input_refs_and_limit(): void
    {
        $read = NodeMother::read();
        $refs = refs(ref('id'));

        $topN = (new TopNRewrite(NodeMother::context()))->of(NodeMother::limit(new Sort($read, $refs), 5));

        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame([$read], $topN->children());
        static::assertSame($refs, $topN->refs);
        static::assertSame(5, $topN->limit);
    }

    public function test_an_external_sort_is_rewritten_up_to_its_run_size_and_kept_beyond_it(): void
    {
        $rewrite = new TopNRewrite(NodeMother::context());
        $sort = new Sort(NodeMother::read(), refs(ref('id')), external_sort()->runSize(10));

        static::assertInstanceOf(TopN::class, $rewrite->of(NodeMother::limit($sort, 10)));
        static::assertInstanceOf(Limit::class, $rewrite->of(NodeMother::limit($sort, 11)));
    }

    public function test_the_configured_sort_decides_when_the_sort_pins_none(): void
    {
        $rewrite = new TopNRewrite(NodeMother::context(config_builder()->sort(external_sort()->runSize(2))->build()));
        $sort = NodeMother::sort(NodeMother::read());

        static::assertInstanceOf(TopN::class, $rewrite->of(NodeMother::limit($sort, 2)));
        static::assertInstanceOf(Limit::class, $rewrite->of(NodeMother::limit($sort, 3)));
    }

    public function test_a_sort_that_pins_its_algorithm_wins_over_the_configured_one(): void
    {
        $rewrite = new TopNRewrite(NodeMother::context(config_builder()->sort(external_sort()->runSize(2))->build()));

        static::assertInstanceOf(
            TopN::class,
            $rewrite->of(NodeMother::limit(new Sort(NodeMother::read(), refs(ref('id')), memory_sort()), 1_000_000)),
        );
    }
}
