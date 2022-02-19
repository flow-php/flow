<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class EntryInstanceOf implements RowCondition
{
    private string $entryName;

    private string $class;

    public function __construct(string $entryName, string $class)
    {
        $this->entryName = $entryName;
        $this->class = $class;
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return \get_class($row->entries()->get($this->entryName)) === $this->class;
    }
}
