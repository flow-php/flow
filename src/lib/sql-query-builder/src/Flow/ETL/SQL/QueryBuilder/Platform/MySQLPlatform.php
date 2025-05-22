<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

/**
 * MySQL-specific SQL platform implementation.
 */
class MySQLPlatform extends GenericPlatform
{
    public function quoteIdentifier(string $identifier): string
    {
        // Handle already quoted identifiers
        if (str_starts_with($identifier, '`') && str_ends_with($identifier, '`')) {
            return $identifier;
        }

        // Handle qualified identifiers (table.column)
        if (str_contains($identifier, '.')) {
            $parts = explode('.', $identifier);
            return implode('.', array_map(fn($part) => '`' . str_replace('`', '``', $part) . '`', $parts));
        }

        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    public function literal(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_string($value)) {
            // MySQL uses backslash escaping in addition to quote doubling
            $escaped = str_replace(['\\', "'"], ['\\\\', "''"], $value);
            return "'" . $escaped . "'";
        }

        if ($value instanceof \DateTimeInterface) {
            return "'" . $value->format('Y-m-d H:i:s') . "'";
        }

        return parent::literal($value);
    }

    protected function buildJoin(array $join): string
    {
        // MySQL 8.0.14+ supports LATERAL
        if (in_array($join['type'], ['INNER LATERAL', 'LEFT LATERAL', 'RIGHT LATERAL'])) {
            $type = str_replace(' LATERAL', '', $join['type']);
            $sql = $type . ' JOIN LATERAL ';
            
            // Handle subqueries
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

        return parent::buildJoin($join);
    }

    public function supportsCTE(): bool
    {
        // MySQL 8.0+ supports CTEs
        return true;
    }

    public function supportsRecursiveCTE(): bool
    {
        // MySQL 8.0+ supports recursive CTEs
        return true;
    }

    public function supportsLateralJoins(): bool
    {
        // MySQL 8.0.14+ supports LATERAL joins
        return true;
    }

    public function buildSQL(array $queryParts): string
    {
        $sql = parent::buildSQL($queryParts);

        // MySQL uses LIMIT with offset syntax differently
        if ($queryParts['limit'] !== null && $queryParts['offset'] !== null) {
            // Remove the separate OFFSET clause added by parent
            $sql = preg_replace('/\nOFFSET \d+$/', '', $sql);
            // Replace LIMIT with LIMIT offset, count syntax
            $sql = preg_replace(
                '/LIMIT (\d+)/',
                sprintf('LIMIT %d, %d', $queryParts['offset'], $queryParts['limit']),
                $sql
            );
        }

        return $sql;
    }

    public function getName(): string
    {
        return 'mysql';
    }
}