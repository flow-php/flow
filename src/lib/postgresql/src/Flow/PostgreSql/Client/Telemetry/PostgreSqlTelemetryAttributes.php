<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Flow-specific PostgreSQL telemetry attribute keys and values.
 *
 * Official database semantic convention keys come from {@see \Flow\Telemetry\SemConvAttributes};
 * per OTel naming guidance flow-custom keys must not extend the `db.` namespace and live under
 * `flow.db.` instead. The same keys are emitted by the Doctrine DBAL instrumentation in
 * flow-php/symfony-telemetry-bundle.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class PostgreSqlTelemetryAttributes
{
    public const string DB_SYSTEM_POSTGRESQL = 'postgresql';

    public const string DB_TRANSACTION_NESTING_LEVEL = 'flow.db.transaction.nesting_level';

    public const string DB_TRANSACTION_SAVEPOINT = 'flow.db.transaction.savepoint';
}
