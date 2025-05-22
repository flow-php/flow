<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Adapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder as DbalQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;
use Flow\ETL\SQL\QueryBuilder\QueryBuilderInterface;

/**
 * Wrapper that implements DBAL QueryBuilder interface but uses Flow QueryBuilder internally.
 * 
 * This allows using Flow QueryBuilder features (CTE, LATERAL JOIN) while maintaining
 * compatibility with code expecting DBAL QueryBuilder.
 */
class FlowQueryBuilderWrapper extends DbalQueryBuilder
{
    private QueryBuilderInterface $flowQueryBuilder;
    private Connection $connection;
    
    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->connection = $connection;
        $this->flowQueryBuilder = new FlowQueryBuilder(
            PlatformFactory::fromConnection($connection)
        );
    }
    
    /**
     * Get the underlying Flow QueryBuilder instance.
     */
    public function getFlowQueryBuilder(): QueryBuilderInterface
    {
        return $this->flowQueryBuilder;
    }
    
    /**
     * Add a Common Table Expression (WITH clause).
     *
     * @param string $name CTE name
     * @param string|QueryBuilderInterface|DbalQueryBuilder $query CTE query
     * @param array<string> $columns Optional column names for the CTE
     * @return self
     */
    public function with(string $name, $query, array $columns = []): self
    {
        if ($query instanceof DbalQueryBuilder) {
            $query = $query->getSQL();
        }
        
        $this->flowQueryBuilder->with($name, $query, $columns);
        
        return $this;
    }
    
    /**
     * Add a recursive Common Table Expression.
     *
     * @param string $name CTE name
     * @param string|QueryBuilderInterface|DbalQueryBuilder $initialQuery Initial query
     * @param string|QueryBuilderInterface|DbalQueryBuilder $recursiveQuery Recursive query
     * @param array<string> $columns Optional column names for the CTE
     * @return self
     */
    public function withRecursive(string $name, $initialQuery, $recursiveQuery, array $columns = []): self
    {
        if ($initialQuery instanceof DbalQueryBuilder) {
            $initialQuery = $initialQuery->getSQL();
        }
        if ($recursiveQuery instanceof DbalQueryBuilder) {
            $recursiveQuery = $recursiveQuery->getSQL();
        }
        
        $this->flowQueryBuilder->withRecursive($name, $initialQuery, $recursiveQuery, $columns);
        
        return $this;
    }
    
    /**
     * Add a LATERAL JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string|QueryBuilderInterface|DbalQueryBuilder $subquery Lateral subquery
     * @param string $alias Subquery alias
     * @param string|null $condition Optional join condition
     * @return self
     */
    public function lateralJoin(string $fromAlias, $subquery, string $alias, ?string $condition = null): self
    {
        if ($subquery instanceof DbalQueryBuilder) {
            $subquery = $subquery->getSQL();
        }
        
        $this->flowQueryBuilder->lateralJoin($fromAlias, $subquery, $alias, $condition);
        
        return $this;
    }
    
    /**
     * Add a LEFT LATERAL JOIN.
     *
     * @param string $fromAlias From table alias
     * @param string|QueryBuilderInterface|DbalQueryBuilder $subquery Lateral subquery
     * @param string $alias Subquery alias
     * @param string|null $condition Optional join condition
     * @return self
     */
    public function leftLateralJoin(string $fromAlias, $subquery, string $alias, ?string $condition = null): self
    {
        if ($subquery instanceof DbalQueryBuilder) {
            $subquery = $subquery->getSQL();
        }
        
        $this->flowQueryBuilder->leftLateralJoin($fromAlias, $subquery, $alias, $condition);
        
        return $this;
    }
    
    /**
     * Get all query parts for programmatic access.
     *
     * @return array
     */
    public function getQueryParts(): array
    {
        return $this->flowQueryBuilder->getQueryParts();
    }
    
    // Override DBAL methods to use Flow QueryBuilder
    
    public function select($select = null, ...$selects): self
    {
        parent::select($select, ...$selects);
        
        $allSelects = is_array($select) ? $select : [$select];
        $allSelects = array_merge($allSelects, $selects);
        
        $this->flowQueryBuilder->select(...$allSelects);
        
        return $this;
    }
    
    public function addSelect($select = null, ...$selects): self
    {
        parent::addSelect($select, ...$selects);
        
        $allSelects = is_array($select) ? $select : [$select];
        $allSelects = array_merge($allSelects, $selects);
        
        $this->flowQueryBuilder->addSelect(...$allSelects);
        
        return $this;
    }
    
    public function from($from, $alias = null): self
    {
        parent::from($from, $alias);
        $this->flowQueryBuilder->from($from, $alias);
        
        return $this;
    }
    
    public function join($fromAlias, $join, $alias, $condition = null): self
    {
        parent::join($fromAlias, $join, $alias, $condition);
        $this->flowQueryBuilder->join($fromAlias, $join, $alias, $condition);
        
        return $this;
    }
    
    public function leftJoin($fromAlias, $join, $alias, $condition = null): self
    {
        parent::leftJoin($fromAlias, $join, $alias, $condition);
        $this->flowQueryBuilder->leftJoin($fromAlias, $join, $alias, $condition);
        
        return $this;
    }
    
    public function rightJoin($fromAlias, $join, $alias, $condition = null): self
    {
        parent::rightJoin($fromAlias, $join, $alias, $condition);
        $this->flowQueryBuilder->rightJoin($fromAlias, $join, $alias, $condition);
        
        return $this;
    }
    
    public function where($predicates): self
    {
        parent::where($predicates);
        $this->flowQueryBuilder->where((string) $predicates);
        
        return $this;
    }
    
    public function andWhere($predicates): self
    {
        parent::andWhere($predicates);
        $this->flowQueryBuilder->andWhere((string) $predicates);
        
        return $this;
    }
    
    public function orWhere($predicates): self
    {
        parent::orWhere($predicates);
        $this->flowQueryBuilder->orWhere((string) $predicates);
        
        return $this;
    }
    
    public function groupBy($groupBy, ...$groupBys): self
    {
        parent::groupBy($groupBy, ...$groupBys);
        
        $allGroupBys = array_merge([$groupBy], $groupBys);
        $this->flowQueryBuilder->groupBy(...$allGroupBys);
        
        return $this;
    }
    
    public function addGroupBy($groupBy, ...$groupBys): self
    {
        parent::addGroupBy($groupBy, ...$groupBys);
        
        $allGroupBys = array_merge([$groupBy], $groupBys);
        $this->flowQueryBuilder->addGroupBy(...$allGroupBys);
        
        return $this;
    }
    
    public function having($predicates): self
    {
        parent::having($predicates);
        $this->flowQueryBuilder->having((string) $predicates);
        
        return $this;
    }
    
    public function andHaving($predicates): self
    {
        parent::andHaving($predicates);
        $this->flowQueryBuilder->andHaving((string) $predicates);
        
        return $this;
    }
    
    public function orHaving($predicates): self
    {
        parent::orHaving($predicates);
        $this->flowQueryBuilder->orHaving((string) $predicates);
        
        return $this;
    }
    
    public function orderBy($sort, $order = null): self
    {
        parent::orderBy($sort, $order);
        $this->flowQueryBuilder->orderBy($sort, $order ?? 'ASC');
        
        return $this;
    }
    
    public function addOrderBy($sort, $order = null): self
    {
        parent::addOrderBy($sort, $order);
        $this->flowQueryBuilder->addOrderBy($sort, $order ?? 'ASC');
        
        return $this;
    }
    
    public function setMaxResults($maxResults): self
    {
        parent::setMaxResults($maxResults);
        
        if ($maxResults !== null) {
            $this->flowQueryBuilder->limit($maxResults);
        }
        
        return $this;
    }
    
    public function setFirstResult($firstResult): self
    {
        parent::setFirstResult($firstResult);
        
        if ($firstResult !== null && $firstResult > 0) {
            $this->flowQueryBuilder->offset($firstResult);
        }
        
        return $this;
    }
    
    public function setParameter($name, $value, $type = null): self
    {
        parent::setParameter($name, $value, $type);
        $this->flowQueryBuilder->setParameter($name, $value, $type);
        
        return $this;
    }
    
    public function setParameters(array $parameters, array $types = []): self
    {
        parent::setParameters($parameters, $types);
        $this->flowQueryBuilder->setParameters($parameters, $types);
        
        return $this;
    }
    
    public function getSQL(): string
    {
        return $this->flowQueryBuilder->toSQL();
    }
    
    public function getParameters(): array
    {
        return $this->flowQueryBuilder->getParameters();
    }
    
    public function getParameterTypes(): array
    {
        return $this->flowQueryBuilder->getParameterTypes();
    }
    
    /**
     * Reset a specific query part - compatible with DBAL's resetQueryPart.
     *
     * @param string $partName
     * @return self
     */
    public function resetQueryPart($partName): self
    {
        parent::resetQueryPart($partName);
        
        // Map DBAL part names to Flow part names
        $mappedPartName = match ($partName) {
            'select' => 'select',
            'from' => 'from',
            'join' => 'joins',
            'where' => 'where',
            'groupBy' => 'groupBy',
            'having' => 'having',
            'orderBy' => 'orderBy',
            default => $partName,
        };
        
        $this->flowQueryBuilder->resetQueryPart($mappedPartName);
        
        return $this;
    }
    
    /**
     * Clone the query builder.
     */
    public function __clone()
    {
        parent::__clone();
        $this->flowQueryBuilder = $this->flowQueryBuilder->clone();
    }
}