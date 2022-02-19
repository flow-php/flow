<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

final class Any implements RowCondition
{
    /**
     * @var RowCondition[]
     */
    private array $conditions;

    public function __construct(RowCondition ...$conditions)
    {
        $this->conditions = $conditions;
    }

    public function isMetFor(Row $row) : bool
    {
        foreach ($this->conditions as $condition) {
            if ($condition->isMetFor($row)) {
                return true;
            }
        }

        return false;
    }
}
