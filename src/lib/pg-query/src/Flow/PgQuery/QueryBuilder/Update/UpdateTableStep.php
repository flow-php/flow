<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

/**
 * Entry point for UPDATE query builder.
 */
interface UpdateTableStep
{
    /**
     * Specify the table to update.
     *
     * @param string $table The table name
     * @param null|string $alias Optional table alias
     */
    public function update(string $table, ?string $alias = null) : UpdateSetStep;
}
