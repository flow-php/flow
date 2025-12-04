<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

interface SelectGroupByStep extends SelectHavingStep
{
    public function groupBy(Expression ...$expressions) : SelectHavingStep;
}
