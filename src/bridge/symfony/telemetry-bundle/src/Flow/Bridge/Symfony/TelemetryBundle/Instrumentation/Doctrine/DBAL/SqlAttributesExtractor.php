<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use function preg_match;
use function strtoupper;
use function trim;

/**
 * Extracts the operation type and target table from a SQL statement for span attributes,
 * following the OpenTelemetry database span naming convention.
 */
final readonly class SqlAttributesExtractor
{
    public function extract(string $sql): SqlAttributes
    {
        $operation = null;
        $matches = [];

        if (preg_match(
            '/^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH|EXPLAIN|CALL|CREATE|ALTER|DROP|TRUNCATE|SAVEPOINT)\b/i',
            $sql,
            $matches,
        )) {
            $operation = strtoupper($matches[1]);
        }

        $collection = null;

        if (preg_match('/\b(?:FROM|INTO|UPDATE|JOIN)\s+(["`\w]+(?:\.["`\w]+)?)/i', $sql, $matches)) {
            $collection = trim($matches[1], '"`');
        }

        return new SqlAttributes($operation, $collection);
    }
}
