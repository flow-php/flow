<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Value object containing extracted query attributes for telemetry.
 *
 * Holds the operation type and target table extracted from a SQL query.
 * Used to avoid redundant regex parsing when building span names and attributes.
 */
final readonly class QueryAttributes
{
    public function __construct(
        public ?string $operation,
        public ?string $target,
    ) {
    }
}
