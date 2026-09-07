<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Schema;

final readonly class BoundPlan
{
    public function __construct(
        private Segments $segments,
        public Schema $schema,
    ) {}

    public function segments(): Segments
    {
        return $this->segments;
    }
}
