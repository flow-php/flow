<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Schema;
use Flow\ETL\Sort\ExternalSort;
use Generator;

final readonly class MergeSortProcessor implements Processor
{
    public function __construct(
        public ExternalSort $sort,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        yield from $this->sort->sort($rows, $context);
    }
}
