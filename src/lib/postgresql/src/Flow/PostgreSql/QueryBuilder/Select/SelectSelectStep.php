<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface SelectSelectStep
{
    /**
     * @param Expression|string ...$expressions
     */
    public function select(string|Expression ...$expressions) : SelectFromStep;

    /**
     * @param Expression|string ...$expressions
     */
    public function selectDistinct(string|Expression ...$expressions) : SelectFromStep;

    /**
     * @param array<Expression|string> $distinctExpressions
     * @param Expression|string ...$selectExpressions
     */
    public function selectDistinctOn(array $distinctExpressions, string|Expression ...$selectExpressions) : SelectFromStep;
}
