<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface SelectHavingStep extends SelectWindowStep
{
    public function having(Condition $condition): SelectWindowStep;
}
