<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

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
     */
    public function __construct(
        public array $steps,
        public array $bound,
        public ?Schema $schema,
    ) {}
}
