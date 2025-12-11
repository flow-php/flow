<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

/**
 * Comparison operators enum.
 */
enum ComparisonOperator : string
{
    case EQ = '=';
    case GT = '>';
    case GTE = '>=';
    case LT = '<';
    case LTE = '<=';
    case NEQ = '<>';
}
