<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\Explain;
use Flow\ETL\Planning\Logical\Format;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Plan;
use Flow\ETL\Planning\Logical\Stage;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\CombineLimits;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class PlanTest extends FlowTestCase
{
    public function test_of_keeps_the_logical_plan_and_the_planner(): void
    {
        $logical = NodeMother::plan(NodeMother::read());
        $planner = config()->planner();

        $plan = Plan::of($logical, $planner);

        static::assertSame($logical, $plan->logical);
        static::assertSame($planner, $plan->planner);
    }

    public function test_to_string_prints_the_tree_its_optimizer_rewrites_by_default(): void
    {
        $plan = Plan::of(
            NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3)),
            config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()->planner(),
        );

        static::assertSame(
            (new Explain())->of(NodeMother::plan(new Limit(NodeMother::read(), 3)), Format::tree),
            $plan->toString(),
        );
    }

    public function test_to_string_prints_the_requested_stage_and_format(): void
    {
        $plan = Plan::of(
            NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3)),
            config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()->planner(),
        );

        static::assertSame(
            (new Explain())->of($plan->logical, Format::boxes),
            $plan->toString(Stage::unoptimized, Format::boxes),
        );
        static::assertNotSame($plan->toString(), $plan->toString(Stage::unoptimized));
    }

    public function test_the_physical_stage_prints_the_plan_the_executor_would_run(): void
    {
        $plan = Plan::of(NodeMother::plan(NodeMother::read()), config()->planner());

        static::assertSame(
            (new Explain())->physical($plan->planner->plan($plan->logical), Format::tree),
            $plan->toString(Stage::physical),
        );
        static::assertStringStartsWith('Physical plan', $plan->toString(Stage::physical));
    }

    public function test_an_anonymous_transformer_is_named_without_its_file(): void
    {
        $plan = df()
            ->read(from_array([['id' => 1]]))
            ->with(new class implements Transformer {
                public static function fromDescription(array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('an anonymous transformer is never described');
                }

                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function transform(Rows $rows, Backend $backend): Rows
                {
                    return $rows;
                }
            })
            ->explain();

        foreach ([Stage::unoptimized, Stage::optimized, Stage::physical] as $stage) {
            static::assertStringNotContainsString("\0", $plan->toString($stage));
            static::assertStringNotContainsString(__FILE__, $plan->toString($stage));
        }

        static::assertStringContainsString('class@anonymous', $plan->toString(Stage::physical));
    }
}
