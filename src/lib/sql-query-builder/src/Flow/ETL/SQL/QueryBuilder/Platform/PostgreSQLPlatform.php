<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

/**
 * PostgreSQL-specific SQL platform implementation.
 */
class PostgreSQLPlatform extends GenericPlatform
{
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

        if ($value instanceof \DateTimeInterface) {
            return "'" . $value->format('Y-m-d H:i:s.u') . "'::timestamp";
        }

        return parent::literal($value);
    }

    protected function buildJoin(array $join): string
    {
        // PostgreSQL supports LATERAL directly
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
            } else {
                $sql .= ' ON TRUE'; // PostgreSQL requires ON clause for LATERAL
            }

            return $sql;
        }

        return parent::buildJoin($join);
    }

    public function getName(): string
    {
        return 'postgresql';
    }
}