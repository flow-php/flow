<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Delete;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

/**
 * Interface for adding RETURNING clause to DELETE query.
 */
interface DeleteReturningStep extends DeleteFinalStep
{
    /**
     * Add a RETURNING clause with specific expressions.
     *
     * @param Expression ...$expressions Expressions to return (columns, functions, etc.)
     */
    public function returning(Expression ...$expressions) : DeleteFinalStep;

    /**
     * Add a RETURNING * clause to return all columns.
     */
    public function returningAll() : DeleteFinalStep;
}
