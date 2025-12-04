<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Condition\Condition;

interface SelectWhereStep extends SelectGroupByStep
{
    public function where(Condition $condition) : SelectGroupByStep;
}
