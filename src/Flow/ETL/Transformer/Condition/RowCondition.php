<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;

interface RowCondition
{
    public function isMetFor(Row $row) : bool;
}
