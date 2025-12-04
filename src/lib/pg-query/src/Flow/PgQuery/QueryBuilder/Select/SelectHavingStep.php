<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Condition\Condition;

interface SelectHavingStep extends SelectWindowStep
{
    public function having(Condition $condition) : SelectWindowStep;
}
