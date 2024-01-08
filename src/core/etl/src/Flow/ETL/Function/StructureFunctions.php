<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row\Reference;

final class StructureFunctions
{
    public function __construct(private readonly Reference $ref)
    {

    }

    public function select(Reference|string ...$refs) : StructureSelect
    {
        return new StructureSelect($this->ref, ...$refs);
    }
}
