<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;

/**
 * Base interface for all SQL conditions (WHERE, HAVING, ON, etc.).
 */
interface Condition extends AstConvertible
{
    public function and(self $other) : AndCondition;

    public function not() : NotCondition;

    public function or(self $other) : OrCondition;
}
