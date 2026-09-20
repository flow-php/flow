<?php

declare(strict_types=1);

namespace Flow\ETL;

final readonly class BoundStep
{
    public function __construct(
        public Processor|Transformer $step,
        public Schema $output,
    ) {}
}
