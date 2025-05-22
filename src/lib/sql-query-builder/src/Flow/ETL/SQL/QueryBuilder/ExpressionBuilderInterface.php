<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder;

/**
 * Interface for building SQL expressions.
 */
interface ExpressionBuilderInterface
{
    /**
     * Create an equality comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function eq(string $column, string $value): string;

    /**
     * Create a not equal comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function neq(string $column, string $value): string;

    /**
     * Create a less than comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function lt(string $column, string $value): string;

    /**
     * Create a less than or equal comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function lte(string $column, string $value): string;

    /**
     * Create a greater than comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function gt(string $column, string $value): string;

    /**
     * Create a greater than or equal comparison.
     *
     * @param string $column Column name
     * @param string $value Parameter placeholder or literal value
     * @return string
     */
    public function gte(string $column, string $value): string;

    /**
     * Create an IN comparison.
     *
     * @param string $column Column name
     * @param array<mixed>|string $values Array of values or parameter placeholder
     * @return string
     */
    public function in(string $column, array|string $values): string;

    /**
     * Create a NOT IN comparison.
     *
     * @param string $column Column name
     * @param array<mixed>|string $values Array of values or parameter placeholder
     * @return string
     */
    public function notIn(string $column, array|string $values): string;

    /**
     * Create an IS NULL comparison.
     *
     * @param string $column Column name
     * @return string
     */
    public function isNull(string $column): string;

    /**
     * Create an IS NOT NULL comparison.
     *
     * @param string $column Column name
     * @return string
     */
    public function isNotNull(string $column): string;

    /**
     * Create a LIKE comparison.
     *
     * @param string $column Column name
     * @param string $pattern Pattern with wildcards
     * @return string
     */
    public function like(string $column, string $pattern): string;

    /**
     * Create a NOT LIKE comparison.
     *
     * @param string $column Column name
     * @param string $pattern Pattern with wildcards
     * @return string
     */
    public function notLike(string $column, string $pattern): string;

    /**
     * Create a BETWEEN comparison.
     *
     * @param string $column Column name
     * @param string $min Minimum value or parameter placeholder
     * @param string $max Maximum value or parameter placeholder
     * @return string
     */
    public function between(string $column, string $min, string $max): string;

    /**
     * Create an AND expression.
     *
     * @param string ...$expressions Expressions to combine
     * @return string
     */
    public function andX(string ...$expressions): string;

    /**
     * Create an OR expression.
     *
     * @param string ...$expressions Expressions to combine
     * @return string
     */
    public function orX(string ...$expressions): string;

    /**
     * Create a NOT expression.
     *
     * @param string $expression Expression to negate
     * @return string
     */
    public function not(string $expression): string;

    /**
     * Quote an identifier (table or column name).
     *
     * @param string $identifier Identifier to quote
     * @return string
     */
    public function quoteIdentifier(string $identifier): string;

    /**
     * Create a literal value (properly escaped).
     *
     * @param mixed $value Value to convert to literal
     * @return string
     */
    public function literal(mixed $value): string;
}