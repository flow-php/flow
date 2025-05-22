<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder;

use Flow\ETL\SQL\QueryBuilder\Platform\PlatformInterface;
use Flow\ETL\SQL\QueryBuilder\Platform\GenericPlatform;

/**
 * SQL Query Builder implementation with support for CTEs and LATERAL JOINs.
 */
class FlowQueryBuilder implements QueryBuilderInterface
{
    private array $queryParts = [
        'ctes' => [],
        'select' => [],
        'from' => null,
        'joins' => [],
        'where' => [],
        'groupBy' => [],
        'having' => [],
        'orderBy' => [],
        'limit' => null,
        'offset' => null,
    ];

    private array $parameters = [];
    private array $parameterTypes = [];
    private ExpressionBuilderInterface $expressionBuilder;
    private PlatformInterface $platform;

    public function __construct(?PlatformInterface $platform = null)
    {
        $this->platform = $platform ?? new GenericPlatform();
        $this->expressionBuilder = new ExpressionBuilder($this->platform);
    }

    public function select(...$columns): self
    {
        $this->queryParts['select'] = [];
        
        return $this->addSelect(...$columns);
    }

    public function addSelect(...$columns): self
    {
        foreach ($columns as $column) {
            $this->queryParts['select'][] = (string) $column;
        }

        return $this;
    }

    public function from(string $table, ?string $alias = null): self
    {
        $this->queryParts['from'] = [
            'table' => $table,
            'alias' => $alias,
        ];

        return $this;
    }

    public function with(string $name, string|QueryBuilderInterface $query, array $columns = []): self
    {
        $sql = $query instanceof QueryBuilderInterface ? $query->toSQL() : $query;
        
        $this->queryParts['ctes'][$name] = [
            'query' => $sql,
            'columns' => $columns,
            'recursive' => false,
        ];

        if ($query instanceof QueryBuilderInterface) {
            // Merge parameters from subquery
            $this->mergeParameters($query);
        }

        return $this;
    }

    public function withRecursive(string $name, string|QueryBuilderInterface $initialQuery, string|QueryBuilderInterface $recursiveQuery, array $columns = []): self
    {
        $initialSql = $initialQuery instanceof QueryBuilderInterface ? $initialQuery->toSQL() : $initialQuery;
        $recursiveSql = $recursiveQuery instanceof QueryBuilderInterface ? $recursiveQuery->toSQL() : $recursiveQuery;
        
        $sql = sprintf('(%s UNION ALL %s)', $initialSql, $recursiveSql);
        
        $this->queryParts['ctes'][$name] = [
            'query' => $sql,
            'columns' => $columns,
            'recursive' => true,
        ];

        if ($initialQuery instanceof QueryBuilderInterface) {
            $this->mergeParameters($initialQuery);
        }
        if ($recursiveQuery instanceof QueryBuilderInterface) {
            $this->mergeParameters($recursiveQuery);
        }

        return $this;
    }

    public function join(string $fromAlias, string $table, string $alias, string $condition): self
    {
        $this->queryParts['joins'][] = [
            'type' => 'INNER',
            'fromAlias' => $fromAlias,
            'table' => $table,
            'alias' => $alias,
            'condition' => $condition,
        ];

        return $this;
    }

    public function leftJoin(string $fromAlias, string $table, string $alias, string $condition): self
    {
        $this->queryParts['joins'][] = [
            'type' => 'LEFT',
            'fromAlias' => $fromAlias,
            'table' => $table,
            'alias' => $alias,
            'condition' => $condition,
        ];

        return $this;
    }

    public function rightJoin(string $fromAlias, string $table, string $alias, string $condition): self
    {
        $this->queryParts['joins'][] = [
            'type' => 'RIGHT',
            'fromAlias' => $fromAlias,
            'table' => $table,
            'alias' => $alias,
            'condition' => $condition,
        ];

        return $this;
    }

    public function lateralJoin(string $fromAlias, string|QueryBuilderInterface $subquery, string $alias, ?string $condition = null): self
    {
        $sql = $subquery instanceof QueryBuilderInterface ? $subquery->toSQL() : $subquery;
        
        $this->queryParts['joins'][] = [
            'type' => 'INNER LATERAL',
            'fromAlias' => $fromAlias,
            'table' => sprintf('(%s)', $sql),
            'alias' => $alias,
            'condition' => $condition,
        ];

        if ($subquery instanceof QueryBuilderInterface) {
            $this->mergeParameters($subquery);
        }

        return $this;
    }

    public function leftLateralJoin(string $fromAlias, string|QueryBuilderInterface $subquery, string $alias, ?string $condition = null): self
    {
        $sql = $subquery instanceof QueryBuilderInterface ? $subquery->toSQL() : $subquery;
        
        $this->queryParts['joins'][] = [
            'type' => 'LEFT LATERAL',
            'fromAlias' => $fromAlias,
            'table' => sprintf('(%s)', $sql),
            'alias' => $alias,
            'condition' => $condition,
        ];

        if ($subquery instanceof QueryBuilderInterface) {
            $this->mergeParameters($subquery);
        }

        return $this;
    }

    public function where(string $condition): self
    {
        $this->queryParts['where'] = [$condition];

        return $this;
    }

    public function andWhere(string $condition): self
    {
        if (empty($this->queryParts['where'])) {
            return $this->where($condition);
        }

        $this->queryParts['where'][] = $condition;

        return $this;
    }

    public function orWhere(string $condition): self
    {
        if (empty($this->queryParts['where'])) {
            return $this->where($condition);
        }

        $existing = $this->queryParts['where'];
        $this->queryParts['where'] = [
            sprintf('(%s) OR (%s)', implode(' AND ', $existing), $condition)
        ];

        return $this;
    }

    public function groupBy(string ...$columns): self
    {
        $this->queryParts['groupBy'] = $columns;

        return $this;
    }

    public function addGroupBy(string ...$columns): self
    {
        $this->queryParts['groupBy'] = array_merge($this->queryParts['groupBy'], $columns);

        return $this;
    }

    public function having(string $condition): self
    {
        $this->queryParts['having'] = [$condition];

        return $this;
    }

    public function andHaving(string $condition): self
    {
        if (empty($this->queryParts['having'])) {
            return $this->having($condition);
        }

        $this->queryParts['having'][] = $condition;

        return $this;
    }

    public function orHaving(string $condition): self
    {
        if (empty($this->queryParts['having'])) {
            return $this->having($condition);
        }

        $existing = $this->queryParts['having'];
        $this->queryParts['having'] = [
            sprintf('(%s) OR (%s)', implode(' AND ', $existing), $condition)
        ];

        return $this;
    }

    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->queryParts['orderBy'] = [
            ['column' => $column, 'direction' => strtoupper($direction)]
        ];

        return $this;
    }

    public function addOrderBy(string $column, string $direction = 'ASC'): self
    {
        $this->queryParts['orderBy'][] = [
            'column' => $column,
            'direction' => strtoupper($direction),
        ];

        return $this;
    }

    public function limit(int $limit): self
    {
        $this->queryParts['limit'] = $limit;

        return $this;
    }

    public function offset(int $offset): self
    {
        $this->queryParts['offset'] = $offset;

        return $this;
    }

    public function setParameter(string $name, mixed $value, ?string $type = null): self
    {
        $this->parameters[$name] = $value;
        
        if ($type !== null) {
            $this->parameterTypes[$name] = $type;
        }

        return $this;
    }

    public function setParameters(array $parameters, array $types = []): self
    {
        $this->parameters = array_merge($this->parameters, $parameters);
        $this->parameterTypes = array_merge($this->parameterTypes, $types);

        return $this;
    }

    public function getQueryParts(): array
    {
        return $this->queryParts;
    }

    public function resetQueryPart(string $partName): self
    {
        if (!array_key_exists($partName, $this->queryParts)) {
            throw new \InvalidArgumentException(sprintf('Unknown query part: %s', $partName));
        }

        $this->queryParts[$partName] = match ($partName) {
            'ctes', 'select', 'joins', 'where', 'groupBy', 'having', 'orderBy' => [],
            'from', 'limit', 'offset' => null,
        };

        return $this;
    }

    public function toSQL(): string
    {
        return $this->platform->buildSQL($this->queryParts);
    }

    public function getParameters(): array
    {
        return $this->parameters;
    }

    public function getParameterTypes(): array
    {
        return $this->parameterTypes;
    }

    public function clone(): self
    {
        $clone = new self($this->platform);
        $clone->queryParts = $this->queryParts;
        $clone->parameters = $this->parameters;
        $clone->parameterTypes = $this->parameterTypes;

        return $clone;
    }

    public function expr(): ExpressionBuilderInterface
    {
        return $this->expressionBuilder;
    }

    /**
     * Merge parameters from another query builder.
     */
    private function mergeParameters(QueryBuilderInterface $queryBuilder): void
    {
        $this->parameters = array_merge($this->parameters, $queryBuilder->getParameters());
        $this->parameterTypes = array_merge($this->parameterTypes, $queryBuilder->getParameterTypes());
    }
}