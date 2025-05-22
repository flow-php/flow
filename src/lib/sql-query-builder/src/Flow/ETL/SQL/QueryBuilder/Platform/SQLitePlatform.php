<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

/**
 * SQLite-specific SQL platform implementation.
 */
class SQLitePlatform extends GenericPlatform
{
    public function quoteIdentifier(string $identifier): string
    {
        // Handle already quoted identifiers
        if (
            (str_starts_with($identifier, '"') && str_ends_with($identifier, '"')) ||
            (str_starts_with($identifier, '`') && str_ends_with($identifier, '`')) ||
            (str_starts_with($identifier, '[') && str_ends_with($identifier, ']'))
        ) {
            return $identifier;
        }

        // Handle qualified identifiers (table.column)
        if (str_contains($identifier, '.')) {
            $parts = explode('.', $identifier);
            return implode('.', array_map(fn($part) => '"' . str_replace('"', '""', $part) . '"', $parts));
        }

        // SQLite supports multiple quoting styles, we'll use double quotes
        return '"' . str_replace('"', '""', $identifier) . '"';
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
            return "'" . str_replace("'", "''", $value) . "'";
        }

        if ($value instanceof \DateTimeInterface) {
            // SQLite stores dates as strings
            return "'" . $value->format('Y-m-d H:i:s') . "'";
        }

        return parent::literal($value);
    }

    protected function buildJoin(array $join): string
    {
        // SQLite does not support LATERAL joins
        if (in_array($join['type'], ['INNER LATERAL', 'LEFT LATERAL', 'RIGHT LATERAL'])) {
            throw new \RuntimeException('SQLite does not support LATERAL joins');
        }

        return parent::buildJoin($join);
    }

    public function supportsCTE(): bool
    {
        // SQLite 3.8.3+ supports CTEs
        return true;
    }

    public function supportsRecursiveCTE(): bool
    {
        // SQLite 3.8.3+ supports recursive CTEs
        return true;
    }

    public function supportsLateralJoins(): bool
    {
        // SQLite does not support LATERAL joins
        return false;
    }

    public function getName(): string
    {
        return 'sqlite';
    }
}