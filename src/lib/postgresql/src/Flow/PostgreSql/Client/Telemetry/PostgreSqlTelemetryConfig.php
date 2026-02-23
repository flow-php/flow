<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

/**
 * Configuration container for PostgreSQL telemetry.
 *
 * Bundles the telemetry instance, clock, and options needed
 * to instrument a PostgreSQL client.
 */
final readonly class PostgreSqlTelemetryConfig
{
    public function __construct(
        public Telemetry $telemetry,
        public ClockInterface $clock,
        public PostgreSqlTelemetryOptions $options = new PostgreSqlTelemetryOptions(),
    ) {
    }
}
