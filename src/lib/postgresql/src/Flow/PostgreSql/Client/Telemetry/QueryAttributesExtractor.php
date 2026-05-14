<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Extracts semantic attributes from SQL queries for telemetry purposes.
 *
 * Parses SQL statements to identify the operation type (SELECT, INSERT, etc.)
 * and target table/collection for OpenTelemetry span attributes.
 */
final readonly class QueryAttributesExtractor
{
    /**
     * Extract both operation and target from a query in a single pass.
     *
     * Returns a QueryAttributes value object containing the operation
     * (SELECT, INSERT, etc.) and target table name. This method is optimized
     * to minimize regex operations per query.
     */
    public function extract(string $query): QueryAttributes
    {
        $operation = null;
        $matches = [];

        if (\preg_match(
            '/^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH|EXPLAIN|COPY|CREATE|ALTER|DROP|TRUNCATE|BEGIN|COMMIT|ROLLBACK)\b/i',
            $query,
            $matches,
        )) {
            $operation = \strtoupper($matches[1]);
        }

        $target = null;

        if (\preg_match('/\b(?:FROM|INTO|UPDATE|JOIN)\s+(["\w]+(?:\.["\w]+)?)/i', $query, $matches)) {
            $target = \trim($matches[1], '"');
        }

        return new QueryAttributes($operation, $target);
    }

    /**
     * Extract the SQL operation from a query.
     *
     * Returns the uppercase operation name (SELECT, INSERT, UPDATE, DELETE, etc.)
     * or null if the operation cannot be determined.
     *
     * @deprecated Use extract() instead for better performance
     */
    public function extractOperation(string $query): ?string
    {
        return $this->extract($query)->operation;
    }

    /**
     * Extract the target table/collection from a query.
     *
     * Attempts to identify the primary table being operated on by looking
     * for FROM, INTO, UPDATE, or JOIN clauses.
     *
     * @deprecated Use extract() instead for better performance
     */
    public function extractTarget(string $query): ?string
    {
        return $this->extract($query)->target;
    }
}
