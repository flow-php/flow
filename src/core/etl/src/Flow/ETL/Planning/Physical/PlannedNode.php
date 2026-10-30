<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Schema;

final readonly class PlannedNode
{
    /**
     * @param list<JoinStep|Loader|Processor|Transformer> $steps as translated
     * @param list<JoinStep|Loader|Processor|Transformer> $bound the same steps after bind(); identical to $steps when this node
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
