<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Loader;
use Flow\ETL\Processor;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

final readonly class PlannedNode
{
    /**
     * @param list<Loader|Processor|Transformer> $steps as translated
     * @param list<Loader|Processor|Transformer> $bound the same steps after bind(); identical to $steps when this node
     *                                                  refused, empty when its input already had no schema - nothing reads
     *                                                  it once the plan refused
     * @param null|Schema $schema this node's output schema; null when the plan refused at or below it
     * @param null|PhysicalPlan $nested the plan this node's rows come from when they come from another plan: a
     *                                  Read over a NestedPlan extractor, or a SideInput. null otherwise.
     */
    public function __construct(
        public array $steps,
        public array $bound,
        public ?Schema $schema,
        public ?PhysicalPlan $nested = null,
    ) {}

    /**
     * @throws InvalidLogicException
     */
    public function nestedOrFail(): PhysicalPlan
    {
        return $this->nested ?? throw InvalidLogicException::because('A SideInput child always has a nested plan');
    }
}
