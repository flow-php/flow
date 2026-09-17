<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineLimits;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\Explain\BoxLayout;
use Flow\ETL\Plan\Explain\FlowLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Explain\TreeLayout;
use Flow\ETL\Plan\Format;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Stage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config_builder;

final class ExplainTest extends FlowTestCase
{
    public function test_the_optimized_tree_is_the_default(): void
    {
        $logical = NodeMother::plan(new Result(new Limit(new Limit(NodeMother::read(), 5), 3)));
        $plan = Plan::of(
            $logical,
            new FlowContext(config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()),
        );

        static::assertSame(
            (new TreeLayout())->render((new Outline())->of(NodeMother::plan(new Result(
                new Limit(NodeMother::read(), 3),
            ))->root)),
            (new Explain())->of($plan),
        );
    }

    public function test_each_format_renders_with_its_layout(): void
    {
        $plan = Plan::of(NodeMother::plan(new Result(NodeMother::limit(NodeMother::read(), 5))), NodeMother::context());
        $outline = (new Outline())->of($plan->logical->root);

        static::assertSame((new TreeLayout())->render($outline), (new Explain())->of($plan, Stage::unoptimized));
        static::assertSame(
            (new BoxLayout())->render($outline),
            (new Explain())->of($plan, Stage::unoptimized, Format::boxes),
        );
        static::assertSame(
            (new FlowLayout())->render($outline),
            (new Explain())->of($plan, Stage::unoptimized, Format::flow),
        );
        static::assertSame(
            (new TreeLayout(declarations: true))->render($outline),
            (new Explain())->of($plan, Stage::unoptimized, Format::declarations),
        );
    }
}
