<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Clause\OrderByItem;

interface SelectOrderByStep extends SelectLimitStep
{
    public function orderBy(OrderByItem ...$items) : SelectLimitStep;
}
