<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row\Reference;

final readonly class ListFunctions
{
    public function __construct(private Reference $ref)
    {
    }

    public function select(Reference|string ...$refs) : ListSelect
    {
        return new ListSelect($this->ref, ...$refs);
    }
}
