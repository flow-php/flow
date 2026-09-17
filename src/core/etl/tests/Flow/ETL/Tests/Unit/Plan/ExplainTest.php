<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\Explain\BoxLayout;
use Flow\ETL\Plan\Explain\FlowLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Explain\TreeLayout;
use Flow\ETL\Plan\Format;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class ExplainTest extends FlowTestCase
{
    public function test_the_tree_is_the_default(): void
    {
        $plan = NodeMother::plan(new Result(NodeMother::limit(NodeMother::read(), 5)));

        static::assertSame((new TreeLayout())->render((new Outline())->of($plan->root)), (new Explain())->of($plan));
    }

    public function test_each_format_renders_with_its_layout(): void
    {
        $plan = NodeMother::plan(new Result(NodeMother::limit(NodeMother::read(), 5)));
        $outline = (new Outline())->of($plan->root);

        static::assertSame((new TreeLayout())->render($outline), (new Explain())->of($plan, Format::tree));
        static::assertSame((new BoxLayout())->render($outline), (new Explain())->of($plan, Format::boxes));
        static::assertSame((new FlowLayout())->render($outline), (new Explain())->of($plan, Format::flow));
        static::assertSame(
            (new TreeLayout(declarations: true))->render($outline),
            (new Explain())->of($plan, Format::declarations),
        );
    }
}
