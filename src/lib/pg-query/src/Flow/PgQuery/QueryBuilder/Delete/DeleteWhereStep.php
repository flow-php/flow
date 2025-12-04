<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Delete;

use Flow\PgQuery\QueryBuilder\Condition\Condition;

/**
 * Interface for adding WHERE clause to DELETE query.
 */
interface DeleteWhereStep extends DeleteReturningStep
{
    /**
     * Add a WHERE clause to filter which rows to delete.
     */
    public function where(Condition $condition) : DeleteReturningStep;
}
