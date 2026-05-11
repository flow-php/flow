<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;

interface SelectOrderByStep extends SelectLimitStep
{
    public function orderBy(OrderBy ...$items): SelectLimitStep;
}
