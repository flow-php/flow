<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

use Flow\PgQuery\QueryBuilder\Condition\Condition;

interface InsertDoUpdateStep extends InsertReturningStep
{
    public function where(Condition $condition) : InsertReturningStep;
}
