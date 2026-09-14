<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Planner\Analysis;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Planner\Rule;
use Flow\ETL\Planner\Rule\CombineLimits;
use Flow\ETL\Planner\Rule\PushFilterIntoSource;
use Flow\ETL\Planner\Rule\PushLimitIntoSource;

use function array_diff;
use function array_values;
use function in_array;
use function sprintf;

final readonly class Planner
{
    /**
     * @var list<Rule>
     */
    private array $rules;

    public function __construct(
        private Lowerings $lowerings,
        Rule ...$rules,
    ) {
        $this->rules = array_values($rules);
    }

    public static function default(): self
    {
        return new self(
            Lowerings::default(),
            new CombineLimits(),
            new PushLimitIntoSource(),
            new PushFilterIntoSource(),
        );
    }

    /**
     * This planner's rules, minus the named ones - so a caller drops one rule without freezing the list
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
            throw new InvalidArgumentException(sprintf('%s is not a registered planner rule', $name));
        }

        return new self($this->lowerings, ...$kept);
    }

    /**
     * @return list<Rule>
     */
    public function rules(): array
    {
        return $this->rules;
    }

    public function plan(LogicalPlan $logical, FlowContext $context): Plan
    {
        return (new Analysis($this->lowerings, new PipelineSplit(), ...$this->rules))->plan($logical, $context);
    }
}
