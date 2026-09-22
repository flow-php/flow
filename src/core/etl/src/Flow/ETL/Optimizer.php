<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Optimizer\JoinSides;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Optimizer\Rule\CombineLimits;
use Flow\ETL\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Plan\LogicalPlan;

use function array_diff;
use function array_values;
use function in_array;
use function sprintf;

final readonly class Optimizer
{
    /**
     * @var list<Rule>
     */
    private array $rules;

    public function __construct(Rule ...$rules)
    {
        $this->rules = array_values($rules);
    }

    public static function default(): self
    {
        return new self(
            new CombineLimits(),
            new CombineSortAndLimit(),
            new PushLimitIntoSource(),
            new PushFilterIntoSource(),
            new CountFromStatistics(),
        );
    }

    /**
     * Rewrites every join's right side as a plan of its own, then $plan with every rule, in order.
     */
    public function optimize(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $plan = $plan->transformUp(new JoinSides($this, $context));

        foreach ($this->rules as $rule) {
            $plan = $rule->apply($plan, $context);
        }

        return $plan;
    }

    /**
     * @return list<Rule>
     */
    public function rules(): array
    {
        return $this->rules;
    }

    /**
     * This optimizer's rules followed by $rules, run after them in the given order.
     *
     * @throws InvalidArgumentException when a rule of the same class is already registered
     */
    public function with(Rule ...$rules): self
    {
        $registered = [];

        foreach ($this->rules as $rule) {
            $registered[] = $rule::class;
        }

        foreach ($rules as $rule) {
            if (in_array($rule::class, $registered, true)) {
                throw new InvalidArgumentException(sprintf('%s is already a registered optimizer rule', $rule::class));
            }

            $registered[] = $rule::class;
        }

        return new self(...$this->rules, ...$rules);
    }

    /**
     * This optimizer's rules, minus the named ones - so a caller drops one rule without freezing the list
     * and silently missing every rule added later.
     *
     * @param class-string<Rule> ...$rules
     *
     * @throws InvalidArgumentException when a name is not a registered rule
     */
    public function without(string ...$rules): self
    {
        $kept = [];
        $registered = [];

        foreach ($this->rules as $rule) {
            $registered[] = $rule::class;

            if (!in_array($rule::class, $rules, true)) {
                $kept[] = $rule;
            }
        }

        foreach (array_diff($rules, $registered) as $name) {
            throw new InvalidArgumentException(sprintf('%s is not a registered optimizer rule', $name));
        }

        return new self(...$kept);
    }
}
