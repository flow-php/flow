<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

/**
 * Step for specifying SET assignments in UPDATE query.
 */
interface UpdateSetStep
{
    /**
     * Set a single column assignment.
     *
     * @param string $column The column name
     * @param Expression $value The value expression
     */
    public function set(string $column, Expression $value) : self;

    /**
     * Set multiple column assignments at once.
     *
     * @param array<string, Expression> $assignments Column => value assignments
     */
    public function setAll(array $assignments) : UpdateFromStep;
}
