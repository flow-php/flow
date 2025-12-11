<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

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
