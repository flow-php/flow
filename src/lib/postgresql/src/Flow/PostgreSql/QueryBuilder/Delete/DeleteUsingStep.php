<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Delete;

use Flow\PostgreSql\QueryBuilder\Table\TableReference;

/**
 * Interface for adding USING clause to DELETE query.
 * The USING clause allows joining other tables for conditional deletes.
 */
interface DeleteUsingStep extends DeleteWhereStep
{
    /**
     * Add a USING clause with one or more table references.
     * This allows deleting rows based on conditions involving other tables.
     *
     * @param TableReference ...$tables Tables to join for conditional deletion
     */
    public function using(TableReference ...$tables) : DeleteWhereStep;
}
