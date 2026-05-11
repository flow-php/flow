<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Table\TableReference;

interface SelectFromStep extends SelectFinalStep
{
    public function from(string|TableReference ...$tables): SelectJoinStep;
}
