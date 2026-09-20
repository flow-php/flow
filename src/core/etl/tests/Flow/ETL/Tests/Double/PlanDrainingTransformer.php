<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Context\ExecutedPlan;
use Flow\ETL\Transformer;

use function iterator_to_array;

/**
 * Drains $plan on every batch - on the context of the plan advancing it that is a cycle, on another context it is a
 * side input.
 */
final class PlanDrainingTransformer implements Transformer
{
    /**
     * @var list<Rows>
     */
    public array $drained = [];

    public function __construct(
        private readonly LogicalPlan $plan,
        private readonly FlowContext $context,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $this->drained = [
            ...$this->drained,
            ...iterator_to_array(ExecutedPlan::of($this->plan, $this->context), false),
        ];

        return $rows;
    }
}
