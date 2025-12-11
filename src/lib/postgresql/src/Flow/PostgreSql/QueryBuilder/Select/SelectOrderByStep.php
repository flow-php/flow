<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Clause\OrderByItem;

interface SelectOrderByStep extends SelectLimitStep
{
    public function orderBy(OrderByItem ...$items) : SelectLimitStep;
}
