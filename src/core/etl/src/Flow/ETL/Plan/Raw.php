<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Plan;

final readonly class Raw implements Plan
{
    public function __construct(
        private Pipeline $root,
        public Refusal $why,
    ) {}

    public function root(): Pipeline
    {
        return $this->root;
    }
}
