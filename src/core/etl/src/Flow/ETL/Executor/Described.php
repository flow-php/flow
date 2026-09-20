<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Schema;

final readonly class Described implements PhysicalPlan
{
    public function __construct(
        private Pipeline $root,
        public Schema $schema,
    ) {}

    public function root(): Pipeline
    {
        return $this->root;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }
}
