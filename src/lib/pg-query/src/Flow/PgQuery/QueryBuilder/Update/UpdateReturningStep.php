<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

/**
 * Step for optionally specifying RETURNING clause in UPDATE query.
 */
interface UpdateReturningStep extends UpdateFinalStep
{
    /**
     * Add RETURNING clause with specific expressions.
     *
     * @param Expression ...$expressions Expressions to return
     */
    public function returning(Expression ...$expressions) : UpdateFinalStep;

    /**
     * Add RETURNING * clause.
     */
    public function returningAll() : UpdateFinalStep;
}
