<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class Opposite implements RowCondition
{
    private RowCondition $condition;

    public function __construct(RowCondition $condition)
    {
        $this->condition = $condition;
    }

    public function isMetFor(Row $row) : bool
    {
        return !$this->condition->isMetFor($row);
    }
}
