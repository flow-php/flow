<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Delete;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

/**
 * Interface for adding WHERE clause to DELETE query.
 */
interface DeleteWhereStep extends DeleteReturningStep
{
    /**
     * Add a WHERE clause to filter which rows to delete.
     */
    public function where(Condition $condition): DeleteReturningStep;
}
