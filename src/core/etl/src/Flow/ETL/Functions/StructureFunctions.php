<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Functions\Reference\Reference;

final readonly class StructureFunctions
{
    public function __construct(
        private Reference $ref,
    ) {}

    public function select(Reference|string ...$refs): StructureSelect
    {
        return new StructureSelect($this->ref, ...$refs);
    }
}
