<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class EntryValueEqualsTo implements RowCondition
{
    public function __construct(
        private readonly string $entryName,
        private readonly mixed $value,
        private readonly bool $identical = true
    ) {
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return $this->identical
            ? $row->valueOf($this->entryName) === $this->value
            : $row->valueOf($this->entryName) == $this->value;
    }
}
