<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Expression\Expression;

interface SelectSelectStep
{
    /**
     * @param Expression ...$expressions
     */
    public function select(Expression ...$expressions) : SelectFromStep;

    /**
     * @param Expression ...$expressions
     */
    public function selectDistinct(Expression ...$expressions) : SelectFromStep;

    /**
     * @param array<Expression> $distinctExpressions
     * @param Expression ...$selectExpressions
     */
    public function selectDistinctOn(array $distinctExpressions, Expression ...$selectExpressions) : SelectFromStep;
}
