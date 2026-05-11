<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionBuilder;

interface SelectWhereStep extends SelectGroupByStep
{
    public function where(Condition|ConditionBuilder $condition): SelectGroupByStep;
}
