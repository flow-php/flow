<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

/**
 * Operation type and target collection parsed from a SQL statement.
 */
final readonly class SqlAttributes
{
    public function __construct(
        public ?string $operation,
        public ?string $collection,
    ) {}
}
