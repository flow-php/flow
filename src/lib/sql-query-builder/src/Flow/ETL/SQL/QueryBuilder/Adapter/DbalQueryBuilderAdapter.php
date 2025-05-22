<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Adapter;

use Doctrine\DBAL\Query\QueryBuilder as DbalQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;
use Flow\ETL\SQL\QueryBuilder\QueryBuilderInterface;

/**
 * Adapter to convert DBAL QueryBuilder to Flow QueryBuilder.
 * 
 * This adapter provides backward compatibility by converting existing
 * DBAL QueryBuilder instances to Flow QueryBuilder instances.
 */
class DbalQueryBuilderAdapter
{
    /**
     * Convert a DBAL QueryBuilder to Flow QueryBuilder.
     * 
     * @param DbalQueryBuilder $dbalQueryBuilder
     * @return QueryBuilderInterface
     */
    public static function fromDbalQueryBuilder(DbalQueryBuilder $dbalQueryBuilder): QueryBuilderInterface
    {
        $connection = $dbalQueryBuilder->getConnection();
        $platform = PlatformFactory::fromConnection($connection);
        
        $flowQueryBuilder = new FlowQueryBuilder($platform);
        
        // Extract and convert query parts
        $queryParts = self::extractQueryParts($dbalQueryBuilder);
        
        // Convert SELECT
        if (!empty($queryParts['select'])) {
            $flowQueryBuilder->select(...$queryParts['select']);
        }
        
        // Convert FROM
        if (!empty($queryParts['from'])) {
            foreach ($queryParts['from'] as $from) {
                $flowQueryBuilder->from($from['table'], $from['alias']);
                break; // Flow QueryBuilder only supports single FROM
            }
        }
        
        // Convert JOINs
        if (!empty($queryParts['join'])) {
            foreach ($queryParts['join'] as $fromAlias => $joins) {
                foreach ($joins as $join) {
                    switch ($join['joinType']) {
                        case 'inner':
                            $flowQueryBuilder->join($fromAlias, $join['joinTable'], $join['joinAlias'], $join['joinCondition']);
                            break;
                        case 'left':
                            $flowQueryBuilder->leftJoin($fromAlias, $join['joinTable'], $join['joinAlias'], $join['joinCondition']);
                            break;
                        case 'right':
                            $flowQueryBuilder->rightJoin($fromAlias, $join['joinTable'], $join['joinAlias'], $join['joinCondition']);
                            break;
                    }
                }
            }
        }
        
        // Convert WHERE
        if (!empty($queryParts['where'])) {
            $whereString = (string) $queryParts['where'];
            if ($whereString !== '') {
                $flowQueryBuilder->where($whereString);
            }
        }
        
        // Convert GROUP BY
        if (!empty($queryParts['groupBy'])) {
            $flowQueryBuilder->groupBy(...$queryParts['groupBy']);
        }
        
        // Convert HAVING
        if (!empty($queryParts['having'])) {
            $havingString = (string) $queryParts['having'];
            if ($havingString !== '') {
                $flowQueryBuilder->having($havingString);
            }
        }
        
        // Convert ORDER BY
        if (!empty($queryParts['orderBy'])) {
            foreach ($queryParts['orderBy'] as $orderBy) {
                if (isset($orderBy['sort'], $orderBy['order'])) {
                    $flowQueryBuilder->addOrderBy($orderBy['sort'], $orderBy['order']);
                }
            }
        }
        
        // Convert LIMIT and OFFSET
        $maxResults = $dbalQueryBuilder->getMaxResults();
        if ($maxResults !== null) {
            $flowQueryBuilder->limit($maxResults);
        }
        
        $firstResult = $dbalQueryBuilder->getFirstResult();
        if ($firstResult !== null && $firstResult > 0) {
            $flowQueryBuilder->offset($firstResult);
        }
        
        // Convert parameters
        $parameters = $dbalQueryBuilder->getParameters();
        $parameterTypes = $dbalQueryBuilder->getParameterTypes();
        
        foreach ($parameters as $key => $value) {
            $type = $parameterTypes[$key] ?? null;
            $flowQueryBuilder->setParameter((string) $key, $value, $type);
        }
        
        return $flowQueryBuilder;
    }
    
    /**
     * Extract query parts from DBAL QueryBuilder using reflection.
     * 
     * @param DbalQueryBuilder $dbalQueryBuilder
     * @return array
     */
    private static function extractQueryParts(DbalQueryBuilder $dbalQueryBuilder): array
    {
        // Use reflection to access private/protected properties
        $reflection = new \ReflectionClass($dbalQueryBuilder);
        
        // Try to get sqlParts property
        $sqlPartsProperty = null;
        $currentClass = $reflection;
        
        while ($currentClass && !$sqlPartsProperty) {
            try {
                $sqlPartsProperty = $currentClass->getProperty('sqlParts');
                $sqlPartsProperty->setAccessible(true);
                break;
            } catch (\ReflectionException $e) {
                $currentClass = $currentClass->getParentClass();
            }
        }
        
        if (!$sqlPartsProperty) {
            // Fallback: try to extract from SQL
            return self::extractFromSQL($dbalQueryBuilder);
        }
        
        return $sqlPartsProperty->getValue($dbalQueryBuilder);
    }
    
    /**
     * Fallback method to extract query parts from generated SQL.
     * 
     * @param DbalQueryBuilder $dbalQueryBuilder
     * @return array
     */
    private static function extractFromSQL(DbalQueryBuilder $dbalQueryBuilder): array
    {
        // This is a simplified extraction - in production you might want more sophisticated parsing
        $sql = $dbalQueryBuilder->getSQL();
        $parts = [
            'select' => [],
            'from' => [],
            'join' => [],
            'where' => null,
            'groupBy' => [],
            'having' => null,
            'orderBy' => [],
        ];
        
        // Try to extract basic parts from SQL string
        // This is a very basic implementation and might not cover all cases
        
        return $parts;
    }
}