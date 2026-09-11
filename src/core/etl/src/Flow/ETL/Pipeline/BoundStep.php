<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Processor;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

final readonly class BoundStep
{
    public function __construct(
        public Processor|Transformer $step,
        public Schema $output,
    ) {}
}
