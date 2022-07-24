<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class EntryValueGreaterThan implements RowCondition
{
    public function __construct(private readonly string $entryName, private readonly mixed $value)
    {
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return $row->valueOf($this->entryName) > $this->value;
    }
}
