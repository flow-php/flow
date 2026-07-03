<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

/**
 * Flow-specific Doctrine DBAL attribute keys.
 *
 * Official database semantic convention keys come from {@see \Flow\Telemetry\SemConvAttributes}
 * and metric names from {@see \Flow\Telemetry\SemConvMetrics}; per OTel naming guidance
 * flow-custom keys must not extend the `db.` namespace and live under `flow.db.` instead.
 * The same keys are emitted by the PostgreSQL client instrumentation in flow-php/postgresql.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class DbAttributes
{
    public const string DB_CONNECTION_NAME = 'flow.db.connection.name';

    public const string DB_TRANSACTION_NESTING_LEVEL = 'flow.db.transaction.nesting_level';
}
