<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

interface InsertReturningStep extends InsertFinalStep
{
    public function returning(Expression ...$expressions) : InsertFinalStep;

    public function returningAll() : InsertFinalStep;
}
