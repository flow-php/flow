<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface InsertDoUpdateStep extends InsertReturningStep
{
    public function where(Condition $condition) : InsertReturningStep;
}
