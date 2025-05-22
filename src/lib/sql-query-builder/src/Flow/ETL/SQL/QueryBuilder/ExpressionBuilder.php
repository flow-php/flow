<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder;

use Flow\ETL\SQL\QueryBuilder\Platform\PlatformInterface;

/**
 * SQL Expression builder for creating complex conditions.
 */
class ExpressionBuilder implements ExpressionBuilderInterface
{
    private PlatformInterface $platform;

    public function __construct(PlatformInterface $platform)
    {
        $this->platform = $platform;
    }

    public function eq(string $column, string $value): string
    {
        return sprintf('%s = %s', $this->quoteIdentifier($column), $value);
    }

    public function neq(string $column, string $value): string
    {
        return sprintf('%s != %s', $this->quoteIdentifier($column), $value);
    }

    public function lt(string $column, string $value): string
    {
        return sprintf('%s < %s', $this->quoteIdentifier($column), $value);
    }

    public function lte(string $column, string $value): string
    {
        return sprintf('%s <= %s', $this->quoteIdentifier($column), $value);
    }

    public function gt(string $column, string $value): string
    {
        return sprintf('%s > %s', $this->quoteIdentifier($column), $value);
    }

    public function gte(string $column, string $value): string
    {
        return sprintf('%s >= %s', $this->quoteIdentifier($column), $value);
    }

    public function in(string $column, array|string $values): string
    {
        if (is_array($values)) {
            if (empty($values)) {
                // Return always false condition for empty array
                return '1 = 0';
            }
            
            $placeholders = array_map([$this, 'literal'], $values);
            $valueList = implode(', ', $placeholders);
        } else {
            $valueList = $values;
        }

        return sprintf('%s IN (%s)', $this->quoteIdentifier($column), $valueList);
    }

    public function notIn(string $column, array|string $values): string
    {
        if (is_array($values)) {
            if (empty($values)) {
                // Return always true condition for empty array
                return '1 = 1';
            }
            
            $placeholders = array_map([$this, 'literal'], $values);
            $valueList = implode(', ', $placeholders);
        } else {
            $valueList = $values;
        }

        return sprintf('%s NOT IN (%s)', $this->quoteIdentifier($column), $valueList);
    }

    public function isNull(string $column): string
    {
        return sprintf('%s IS NULL', $this->quoteIdentifier($column));
    }

    public function isNotNull(string $column): string
    {
        return sprintf('%s IS NOT NULL', $this->quoteIdentifier($column));
    }

    public function like(string $column, string $pattern): string
    {
        return sprintf('%s LIKE %s', $this->quoteIdentifier($column), $pattern);
    }

    public function notLike(string $column, string $pattern): string
    {
        return sprintf('%s NOT LIKE %s', $this->quoteIdentifier($column), $pattern);
    }

    public function between(string $column, string $min, string $max): string
    {
        return sprintf('%s BETWEEN %s AND %s', $this->quoteIdentifier($column), $min, $max);
    }

    public function andX(string ...$expressions): string
    {
        if (empty($expressions)) {
            return '1 = 1';
        }

        return '(' . implode(' AND ', $expressions) . ')';
    }

    public function orX(string ...$expressions): string
    {
        if (empty($expressions)) {
            return '1 = 0';
        }

        return '(' . implode(' OR ', $expressions) . ')';
    }

    public function not(string $expression): string
    {
        return sprintf('NOT (%s)', $expression);
    }

    public function quoteIdentifier(string $identifier): string
    {
        return $this->platform->quoteIdentifier($identifier);
    }

    public function literal(mixed $value): string
    {
        return $this->platform->literal($value);
    }
}