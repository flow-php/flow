<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Values\Schema;

final readonly class Inferred
{
    public function __construct(
        public Schema $schema,
        public SampledUnits $units,
    ) {}
}
