<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface SelectWhereStep extends SelectGroupByStep
{
    public function where(Condition $condition) : SelectGroupByStep;
}
