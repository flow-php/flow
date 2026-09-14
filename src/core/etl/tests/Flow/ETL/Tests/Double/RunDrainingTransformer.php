<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Execution\Run;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

use function iterator_to_array;

/**
 * Drains $run over $plan on every batch - the same Run as the one advancing it is a cycle, another Run is a side
 * input.
 */
final class RunDrainingTransformer implements Transformer
{
    /**
     * @var list<Rows>
     */
    public array $drained = [];

    public function __construct(
        private readonly Run $run,
        private readonly LogicalPlan $plan,
        private readonly FlowContext $context,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $this->drained = [...$this->drained, ...iterator_to_array($this->run->of($this->plan, $this->context), false)];

        return $rows;
    }
}
