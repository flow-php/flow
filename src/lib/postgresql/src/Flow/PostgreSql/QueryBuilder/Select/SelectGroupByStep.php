<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface SelectGroupByStep extends SelectHavingStep
{
    public function groupBy(Expression ...$expressions) : SelectHavingStep;
}
