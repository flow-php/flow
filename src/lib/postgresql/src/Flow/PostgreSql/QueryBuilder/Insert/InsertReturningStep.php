<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface InsertReturningStep extends InsertFinalStep
{
    public function returning(Expression ...$expressions) : InsertFinalStep;

    public function returningAll() : InsertFinalStep;
}
