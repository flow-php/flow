<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder;

/**
 * Interface for SQL Query Builder with support for advanced SQL features.
 */
interface QueryBuilderInterface
{
    /**
     * Add SELECT columns to the query.
     *
     * @param mixed ...$columns Column names or expressions
     * @return self
     */
    public function select(...$columns): self;

    /**
     * Add additional SELECT columns to the query.
     *
     * @param mixed ...$columns Column names or expressions
     * @return self
     */
    public function addSelect(...$columns): self;

    /**
     * Set the FROM table.
     *
     * @param string $table Table name
     * @param string|null $alias Optional table alias
     * @return self
     */
    public function from(string $table, ?string $alias = null): self;

    /**
     * Add a Common Table Expression (WITH clause).
     *
     * @param string $name CTE name
     * @param string|QueryBuilderInterface $query CTE query (string SQL or QueryBuilder)
     * @param array<string> $columns Optional column names for the CTE
     * @return self
     */
    public function with(string $name, string|QueryBuilderInterface $query, array $columns = []): self;

    /**
     * Add a recursive Common Table Expression.
     *
     * @param string $name CTE name
     * @param string|QueryBuilderInterface $initialQuery Initial query
     * @param string|QueryBuilderInterface $recursiveQuery Recursive query
     * @param array<string> $columns Optional column names for the CTE
     * @return self
     */
    public function withRecursive(string $name, string|QueryBuilderInterface $initialQuery, string|QueryBuilderInterface $recursiveQuery, array $columns = []): self;

    /**
     * Add an INNER JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string $table Join table name
     * @param string $alias Join table alias
     * @param string $condition Join condition
     * @return self
     */
    public function join(string $fromAlias, string $table, string $alias, string $condition): self;

    /**
     * Add a LEFT JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string $table Join table name
     * @param string $alias Join table alias
     * @param string $condition Join condition
     * @return self
     */
    public function leftJoin(string $fromAlias, string $table, string $alias, string $condition): self;

    /**
     * Add a RIGHT JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string $table Join table name
     * @param string $alias Join table alias
     * @param string $condition Join condition
     * @return self
     */
    public function rightJoin(string $fromAlias, string $table, string $alias, string $condition): self;

    /**
     * Add a LATERAL JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string|QueryBuilderInterface $subquery Lateral subquery
     * @param string $alias Subquery alias
     * @param string|null $condition Optional join condition
     * @return self
     */
    public function lateralJoin(string $fromAlias, string|QueryBuilderInterface $subquery, string $alias, ?string $condition = null): self;

    /**
     * Add a LEFT LATERAL JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string|QueryBuilderInterface $subquery Lateral subquery
     * @param string $alias Subquery alias
     * @param string|null $condition Optional join condition
     * @return self
     */
    public function leftLateralJoin(string $fromAlias, string|QueryBuilderInterface $subquery, string $alias, ?string $condition = null): self;

    /**
     * Add WHERE condition.
     *
     * @param string $condition WHERE condition
     * @return self
     */
    public function where(string $condition): self;

    /**
     * Add AND WHERE condition.
     *
     * @param string $condition WHERE condition
     * @return self
     */
    public function andWhere(string $condition): self;

    /**
     * Add OR WHERE condition.
     *
     * @param string $condition WHERE condition
     * @return self
     */
    public function orWhere(string $condition): self;

    /**
     * Add GROUP BY columns.
     *
     * @param string ...$columns Column names
     * @return self
     */
    public function groupBy(string ...$columns): self;

    /**
     * Add additional GROUP BY columns.
     *
     * @param string ...$columns Column names
     * @return self
     */
    public function addGroupBy(string ...$columns): self;

    /**
     * Add HAVING condition.
     *
     * @param string $condition HAVING condition
     * @return self
     */
    public function having(string $condition): self;

    /**
     * Add AND HAVING condition.
     *
     * @param string $condition HAVING condition
     * @return self
     */
    public function andHaving(string $condition): self;

    /**
     * Add OR HAVING condition.
     *
     * @param string $condition HAVING condition
     * @return self
     */
    public function orHaving(string $condition): self;

    /**
     * Add ORDER BY.
     *
     * @param string $column Column name or expression
     * @param string $direction Sort direction (ASC or DESC)
     * @return self
     */
    public function orderBy(string $column, string $direction = 'ASC'): self;

    /**
     * Add additional ORDER BY.
     *
     * @param string $column Column name or expression
     * @param string $direction Sort direction (ASC or DESC)
     * @return self
     */
    public function addOrderBy(string $column, string $direction = 'ASC'): self;

    /**
     * Set query limit.
     *
     * @param int $limit Maximum number of rows
     * @return self
     */
    public function limit(int $limit): self;

    /**
     * Set query offset.
     *
     * @param int $offset Number of rows to skip
     * @return self
     */
    public function offset(int $offset): self;

    /**
     * Set a query parameter.
     *
     * @param string $name Parameter name (without : prefix)
     * @param mixed $value Parameter value
     * @param string|null $type Optional parameter type
     * @return self
     */
    public function setParameter(string $name, mixed $value, ?string $type = null): self;

    /**
     * Set multiple query parameters.
     *
     * @param array<string, mixed> $parameters Parameters array
     * @param array<string, string> $types Optional parameter types
     * @return self
     */
    public function setParameters(array $parameters, array $types = []): self;

    /**
     * Get all query parts.
     *
     * @return array{
     *     ctes: array<string, array{query: string, columns: array<string>, recursive: bool}>,
     *     select: array<string>,
     *     from: array{table: string, alias: string|null},
     *     joins: array<array{type: string, table: string, alias: string, condition: string|null, fromAlias: string}>,
     *     where: array<string>,
     *     groupBy: array<string>,
     *     having: array<string>,
     *     orderBy: array<array{column: string, direction: string}>,
     *     limit: int|null,
     *     offset: int|null
     * }
     */
    public function getQueryParts(): array;

    /**
     * Reset specific query part.
     *
     * @param string $partName Query part name
     * @return self
     */
    public function resetQueryPart(string $partName): self;

    /**
     * Generate SQL string for the current query state.
     *
     * @return string SQL query string
     */
    public function toSQL(): string;

    /**
     * Get all query parameters.
     *
     * @return array<string, mixed>
     */
    public function getParameters(): array;

    /**
     * Get parameter types.
     *
     * @return array<string, string>
     */
    public function getParameterTypes(): array;

    /**
     * Clone the query builder.
     *
     * @return self
     */
    public function clone(): self;

    /**
     * Create expression builder for complex conditions.
     *
     * @return ExpressionBuilderInterface
     */
    public function expr(): ExpressionBuilderInterface;
}