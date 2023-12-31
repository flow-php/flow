<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row\Reference;

final class ListFunctions
{
    public function __construct(private readonly Reference $ref)
    {
    }

    public function select(Reference|string ...$refs) : ListSelect
    {
        return new ListSelect($this->ref, ...$refs);
    }
}
