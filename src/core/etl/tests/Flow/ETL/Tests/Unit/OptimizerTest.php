<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use ArrayObject;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineLimits;
use Flow\ETL\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class OptimizerTest extends FlowTestCase
{
    public function test_optimize_applies_every_rule_once_in_registration_order(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $plan = NodeMother::plan(NodeMother::read());

        $optimized = (new Optimizer(new RecordingRule('first', $log), new RecordingRule('second', $log)))->optimize(
            $plan,
            NodeMother::context(),
        );

        static::assertSame(['first', 'second'], $log->getArrayCopy());
        static::assertSame($plan, $optimized);
    }

    public function test_optimize_applies_the_rules_to_a_joins_right_side_first(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $plan = NodeMother::plan(NodeMother::crossJoin(NodeMother::read(), NodeMother::plan(NodeMother::read())->root));

        (new Optimizer(new RecordingRule('first', $log), new RecordingRule('second', $log)))->optimize(
            $plan,
            NodeMother::context(),
        );

        static::assertSame(['first', 'second', 'first', 'second'], $log->getArrayCopy());
    }

    public function test_an_optimizer_without_rules_returns_the_plan_it_was_given(): void
    {
        $plan = NodeMother::plan(NodeMother::read());

        static::assertSame($plan, (new Optimizer())->optimize($plan, NodeMother::context()));
    }

    public function test_default_registers_combine_limits_combine_sort_and_limit_push_limit_push_filter_then_count_from_statistics(): void
    {
        static::assertSame(
            [
                CombineLimits::class,
                CombineSortAndLimit::class,
                PushLimitIntoSource::class,
                PushFilterIntoSource::class,
                CountFromStatistics::class,
            ],
            array_map(static fn($rule) => $rule::class, Optimizer::default()->rules()),
        );
    }

    public function test_without_drops_the_named_rule_and_keeps_the_rest(): void
    {
        static::assertSame(
            [CombineLimits::class, CombineSortAndLimit::class, PushFilterIntoSource::class, CountFromStatistics::class],
            array_map(
                static fn($rule) => $rule::class,
                Optimizer::default()->without(PushLimitIntoSource::class)->rules(),
            ),
        );
    }

    public function test_without_returns_a_new_optimizer(): void
    {
        $optimizer = Optimizer::default();

        static::assertNotSame($optimizer, $optimizer->without(CombineLimits::class));
    }

    public function test_without_throws_on_an_unregistered_rule_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(RecordingRule::class . ' is not a registered optimizer rule');

        Optimizer::default()->without(RecordingRule::class);
    }

    public function test_with_appends_the_rules_after_the_registered_ones(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();

        static::assertSame(
            [CombineLimits::class, RecordingRule::class],
            array_map(
                static fn($rule) => $rule::class,
                (new Optimizer(new CombineLimits()))
                    ->with(new RecordingRule('first', $log))
                    ->rules(),
            ),
        );
    }

    public function test_with_returns_a_new_optimizer(): void
    {
        $optimizer = new Optimizer();

        static::assertNotSame($optimizer, $optimizer->with(new CombineLimits()));
    }

    public function test_with_throws_on_a_rule_class_already_registered(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(CombineLimits::class . ' is already a registered optimizer rule');

        Optimizer::default()->with(new CombineLimits());
    }

    public function test_with_throws_on_the_same_rule_class_given_twice(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(CombineLimits::class . ' is already a registered optimizer rule');

        (new Optimizer())->with(new CombineLimits(), new CombineLimits());
    }
}
