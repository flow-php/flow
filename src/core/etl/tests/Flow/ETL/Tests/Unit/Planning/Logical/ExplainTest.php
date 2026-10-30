<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical;

use Flow\ETL\Planning\Explain\BoxLayout;
use Flow\ETL\Planning\Explain\FlowLayout;
use Flow\ETL\Planning\Explain\LogicalOutline;
use Flow\ETL\Planning\Explain\TreeLayout;
use Flow\ETL\Planning\Logical\Explain;
use Flow\ETL\Planning\Logical\Format;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class ExplainTest extends FlowTestCase
{
    public function test_the_tree_is_the_default(): void
    {
        $plan = NodeMother::plan(new Result(NodeMother::limit(NodeMother::read(), 5)));

        static::assertSame(
            (new TreeLayout())->render((new LogicalOutline())->of($plan->root)),
            (new Explain())->of($plan),
        );
    }

    public function test_each_format_renders_with_its_layout(): void
    {
        $plan = NodeMother::plan(new Result(NodeMother::limit(NodeMother::read(), 5)));
        $outline = (new LogicalOutline())->of($plan->root);

        static::assertSame((new TreeLayout())->render($outline), (new Explain())->of($plan, Format::tree));
        static::assertSame((new BoxLayout())->render($outline), (new Explain())->of($plan, Format::boxes));
        static::assertSame((new FlowLayout())->render($outline), (new Explain())->of($plan, Format::flow));
        static::assertSame(
            (new TreeLayout())->render((new LogicalOutline(declarations: true))->of($plan->root)),
            (new Explain())->of($plan, Format::declarations),
        );
    }
}
