<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Clause\OnConflictClause;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface InsertOnConflictStep extends InsertReturningStep
{
    public function onConflict(OnConflictClause $clause): InsertReturningStep;

    public function onConflictDoNothing(?ConflictTarget $target = null): InsertReturningStep;

    /**
     * @param array<string, Expression> $updates
     */
    public function onConflictDoUpdate(ConflictTarget $target, array $updates): InsertDoUpdateStep;
}
