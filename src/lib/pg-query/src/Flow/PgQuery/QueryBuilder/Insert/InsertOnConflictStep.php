<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

use Flow\PgQuery\QueryBuilder\Clause\{ConflictTarget, OnConflictClause};
use Flow\PgQuery\QueryBuilder\Expression\Expression;

interface InsertOnConflictStep extends InsertReturningStep
{
    public function onConflict(OnConflictClause $clause) : InsertReturningStep;

    public function onConflictDoNothing(?ConflictTarget $target = null) : InsertReturningStep;

    /**
     * @param array<string, Expression> $updates
     */
    public function onConflictDoUpdate(ConflictTarget $target, array $updates) : InsertDoUpdateStep;
}
