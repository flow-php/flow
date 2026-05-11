<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Delete;

use Flow\PostgreSql\QueryBuilder\Table\Table;

/**
 * Entry point interface for DELETE query builder.
 * Requires specifying the table to delete from.
 */
interface DeleteFromStep
{
    /**
     * Specify the table to delete from.
     *
     * @param string|Table $table Table name, "schema.table" string, or Table reference
     * @param null|string $alias Optional table alias
     */
    public function from(string|Table $table, ?string $alias = null): DeleteUsingStep;
}
