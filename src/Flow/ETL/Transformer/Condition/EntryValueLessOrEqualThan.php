<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class EntryValueLessOrEqualThan implements RowCondition
{
    private string $entryName;

    /**
     * @var mixed
     */
    private $value;

    /**
     * @param string $entryName
     * @param mixed $value
     */
    public function __construct(string $entryName, $value)
    {
        $this->entryName = $entryName;
        $this->value = $value;
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return $row->valueOf($this->entryName) <= $this->value;
    }
}
