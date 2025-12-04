<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Delete;

/**
 * Entry point interface for DELETE query builder.
 * Requires specifying the table to delete from.
 */
interface DeleteFromStep
{
    /**
     * Specify the table to delete from.
     *
     * @param string $table Table name
     * @param null|string $alias Optional table alias
     */
    public function from(string $table, ?string $alias = null) : DeleteUsingStep;
}
