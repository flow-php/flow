<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Plan;
use Flow\ETL\Schema;

final readonly class Described implements Plan
{
    public function __construct(
        private Pipeline $root,
        public Schema $schema,
    ) {}

    public function root(): Pipeline
    {
        return $this->root;
    }
}
