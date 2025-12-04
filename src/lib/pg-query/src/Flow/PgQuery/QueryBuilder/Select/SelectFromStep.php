<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Table\TableReference;

interface SelectFromStep extends SelectFinalStep
{
    public function from(TableReference ...$tables) : SelectJoinStep;
}
