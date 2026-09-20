<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;

final readonly class Raw implements PhysicalPlan
{
    /**
     * @param null|Schema $schema the rows the plan returns, when the refusal came from elsewhere - a sink
     */
    public function __construct(
        private Pipeline $root,
        public SchemaNotDerivableException $why,
        public ?Schema $schema = null,
    ) {}

    public function root(): Pipeline
    {
        return $this->root;
    }

    public function schema(): Schema
    {
        return $this->schema ?? throw $this->why;
    }
}
