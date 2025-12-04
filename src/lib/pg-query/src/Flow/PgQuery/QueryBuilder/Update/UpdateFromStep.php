<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

use Flow\PgQuery\QueryBuilder\Table\TableReference;

/**
 * Step for optionally specifying FROM clause in UPDATE query.
 */
interface UpdateFromStep extends UpdateWhereStep
{
    /**
     * Add FROM clause for JOINed updates.
     *
     * @param TableReference ...$tables Tables to reference in FROM clause
     */
    public function from(TableReference ...$tables) : UpdateWhereStep;
}
