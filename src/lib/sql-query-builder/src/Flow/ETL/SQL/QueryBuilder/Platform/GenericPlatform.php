<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

/**
 * Generic SQL platform implementation.
 */
class GenericPlatform implements PlatformInterface
{
    public function buildSQL(array $queryParts): string
    {
        $sql = [];

        // Build CTEs
        if (!empty($queryParts['ctes'])) {
            $sql[] = $this->buildCTEs($queryParts['ctes']);
        }

        // Build SELECT
        if (!empty($queryParts['select'])) {
            $sql[] = 'SELECT ' . implode(', ', $queryParts['select']);
        } else {
            $sql[] = 'SELECT *';
        }

        // Build FROM
        if ($queryParts['from'] !== null) {
            $from = $this->quoteIdentifier($queryParts['from']['table']);
            if ($queryParts['from']['alias'] !== null) {
                $from .= ' AS ' . $this->quoteIdentifier($queryParts['from']['alias']);
            }
            $sql[] = 'FROM ' . $from;
        }

        // Build JOINs
        foreach ($queryParts['joins'] as $join) {
            $sql[] = $this->buildJoin($join);
        }

        // Build WHERE
        if (!empty($queryParts['where'])) {
            $sql[] = 'WHERE ' . implode(' AND ', $queryParts['where']);
        }

        // Build GROUP BY
        if (!empty($queryParts['groupBy'])) {
            $sql[] = 'GROUP BY ' . implode(', ', array_map([$this, 'quoteIdentifier'], $queryParts['groupBy']));
        }

        // Build HAVING
        if (!empty($queryParts['having'])) {
            $sql[] = 'HAVING ' . implode(' AND ', $queryParts['having']);
        }

        // Build ORDER BY
        if (!empty($queryParts['orderBy'])) {
            $orderParts = [];
            foreach ($queryParts['orderBy'] as $order) {
                $orderParts[] = $this->quoteIdentifier($order['column']) . ' ' . $order['direction'];
            }
            $sql[] = 'ORDER BY ' . implode(', ', $orderParts);
        }

        // Build LIMIT/OFFSET
        if ($queryParts['limit'] !== null) {
            $sql[] = 'LIMIT ' . $queryParts['limit'];
        }
        if ($queryParts['offset'] !== null) {
            $sql[] = 'OFFSET ' . $queryParts['offset'];
        }

        return implode("\n", $sql);
    }

    protected function buildCTEs(array $ctes): string
    {
        $cteParts = [];
        $hasRecursive = false;

        foreach ($ctes as $name => $cte) {
            if ($cte['recursive']) {
                $hasRecursive = true;
            }

            $cteSql = $this->quoteIdentifier($name);
            
            if (!empty($cte['columns'])) {
                $cteSql .= ' (' . implode(', ', array_map([$this, 'quoteIdentifier'], $cte['columns'])) . ')';
            }
            
            $cteSql .= ' AS ' . $cte['query'];
            $cteParts[] = $cteSql;
        }

        $withClause = $hasRecursive ? 'WITH RECURSIVE ' : 'WITH ';
        
        return $withClause . implode(', ', $cteParts);
    }

    protected function buildJoin(array $join): string
    {
        $sql = $join['type'] . ' JOIN ';
        
        // Handle subqueries (for LATERAL joins)
        if (str_starts_with($join['table'], '(') && str_ends_with($join['table'], ')')) {
            $sql .= $join['table'];
        } else {
            $sql .= $this->quoteIdentifier($join['table']);
        }
        
        $sql .= ' AS ' . $this->quoteIdentifier($join['alias']);
        
        if ($join['condition'] !== null) {
            $sql .= ' ON ' . $join['condition'];
        }

        return $sql;
    }

    public function quoteIdentifier(string $identifier): string
    {
        // Handle already quoted identifiers
        if (str_starts_with($identifier, '"') && str_ends_with($identifier, '"')) {
            return $identifier;
        }

        // Handle qualified identifiers (table.column)
        if (str_contains($identifier, '.')) {
            $parts = explode('.', $identifier);
            return implode('.', array_map(fn($part) => '"' . str_replace('"', '""', $part) . '"', $parts));
        }

        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    public function literal(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_string($value)) {
            return "'" . str_replace("'", "''", $value) . "'";
        }

        if (is_array($value)) {
            throw new \InvalidArgumentException('Cannot convert array to SQL literal');
        }

        if (is_object($value)) {
            if (method_exists($value, '__toString')) {
                return $this->literal((string) $value);
            }
            throw new \InvalidArgumentException('Cannot convert object to SQL literal');
        }

        throw new \InvalidArgumentException(sprintf('Unsupported value type: %s', gettype($value)));
    }

    public function supportsCTE(): bool
    {
        return true;
    }

    public function supportsRecursiveCTE(): bool
    {
        return true;
    }

    public function supportsLateralJoins(): bool
    {
        return true;
    }

    public function getName(): string
    {
        return 'generic';
    }
}