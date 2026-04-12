<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Update;

use Flow\PostgreSql\QueryBuilder\Table\Table;

/**
 * Entry point for UPDATE query builder.
 */
interface UpdateTableStep
{
    /**
     * Specify the table to update.
     *
     * @param string|Table $table The table name, "schema.table" string, or Table reference
     * @param null|string $alias Optional table alias
     */
    public function update(string|Table $table, ?string $alias = null) : UpdateSetStep;
}
