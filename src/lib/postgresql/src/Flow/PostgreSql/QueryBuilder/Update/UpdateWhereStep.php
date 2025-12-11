<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Update;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

/**
 * Step for optionally specifying WHERE clause in UPDATE query.
 */
interface UpdateWhereStep extends UpdateReturningStep
{
    /**
     * Add WHERE clause to filter rows.
     *
     * @param Condition $condition The condition to apply
     */
    public function where(Condition $condition) : UpdateReturningStep;
}
