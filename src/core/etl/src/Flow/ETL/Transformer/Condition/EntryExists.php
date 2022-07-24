<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class EntryExists implements RowCondition
{
    public function __construct(private readonly string $entryName)
    {
    }

    public function isMetFor(Row $row) : bool
    {
        return $row->entries()->has($this->entryName);
    }
}
