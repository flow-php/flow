<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Optimizer\TopNRewrite;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class TopNRewriteTest extends FlowTestCase
{
    public function test_a_node_that_is_not_a_limit_is_returned_as_is(): void
    {
        $sort = NodeMother::sort(NodeMother::read());

        static::assertSame($sort, (new TopNRewrite())->of($sort));
    }

    public function test_a_limit_over_anything_but_a_sort_is_returned_as_is(): void
    {
        $limit = NodeMother::limit(NodeMother::select(NodeMother::sort(NodeMother::read())), 5);

        static::assertSame($limit, (new TopNRewrite())->of($limit));
    }

    public function test_a_limit_over_a_sort_becomes_a_top_n_over_the_sorts_input_refs_and_limit(): void
    {
        $read = NodeMother::read();
        $refs = refs(ref('id'));

        $topN = (new TopNRewrite())->of(NodeMother::limit(new Sort($read, $refs), 5));

        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame([$read], $topN->children());
        static::assertSame($refs, $topN->refs);
        static::assertSame(5, $topN->limit);
    }

    public function test_a_limit_over_any_sort_is_rewritten_and_keeps_the_pinned_algorithm(): void
    {
        $algorithm = external_sort()->memoryLimit(Unit::fromMb(1));

        $topN = (new TopNRewrite())->of(NodeMother::limit(
            new Sort(NodeMother::read(), refs(ref('id')), $algorithm),
            1_000_000,
        ));

        static::assertInstanceOf(TopN::class, $topN);
        static::assertSame($algorithm, $topN->algorithm);
    }
}
