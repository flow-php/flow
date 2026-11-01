<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

/**
 * Custom keys only, under `flow.db.`; also emitted by flow-php/postgresql instrumentation.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class DbAttributes
{
    public const string DB_CONNECTION_NAME = 'flow.db.connection.name';

    public const string DB_TRANSACTION_NESTING_LEVEL = 'flow.db.transaction.nesting_level';
}
