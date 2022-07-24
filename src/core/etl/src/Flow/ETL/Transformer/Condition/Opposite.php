<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class Opposite implements RowCondition
{
    public function __construct(private readonly RowCondition $condition)
    {
    }

    public function isMetFor(Row $row) : bool
    {
        return !$this->condition->isMetFor($row);
    }
}
