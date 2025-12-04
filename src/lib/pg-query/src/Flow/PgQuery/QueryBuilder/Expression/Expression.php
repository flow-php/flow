<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\QueryBuilder\Bridge\AstConvertible;

/**
 * Base interface for all SQL expressions.
 */
interface Expression extends AstConvertible
{
    /**
     * Create an aliased expression: expr AS alias.
     */
    public function as(string $alias) : AliasedExpression;
}
