<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;

/**
 * Base interface for all SQL expressions.
 */
interface Expression extends AstConvertible
{
    /**
     * Create an aliased expression: expr AS alias.
     */
    public function as(string $alias): AliasedExpression;
}
