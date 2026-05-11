<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

/**
 * Base interface for all SQL conditions (WHERE, HAVING, ON, etc.).
 *
 * Conditions are expressions that evaluate to boolean — they can be used
 * anywhere an expression is expected (SELECT list, CASE WHEN, etc.).
 */
interface Condition extends Expression
{
    public function and(self $other): AndCondition;

    public function not(): NotCondition;

    public function or(self $other): OrCondition;
}
