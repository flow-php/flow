<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Custom keys live under `flow.db.`, shared with the DBAL instrumentation.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class PostgreSqlTelemetryAttributes
{
    public const string DB_SYSTEM_POSTGRESQL = 'postgresql';

    public const string DB_TRANSACTION_NESTING_LEVEL = 'flow.db.transaction.nesting_level';

    public const string DB_TRANSACTION_SAVEPOINT = 'flow.db.transaction.savepoint';
}
