<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

/**
 * OpenTelemetry semantic convention attribute keys emitted by the Doctrine DBAL instrumentation.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/database/database-spans/
 */
final class DbAttributes
{
    public const string DB_COLLECTION_NAME = 'db.collection.name';

    public const string DB_CONNECTION_NAME = 'db.connection.name';

    public const string DB_NAMESPACE = 'db.namespace';

    public const string DB_OPERATION_NAME = 'db.operation.name';

    public const string DB_QUERY_PARAMETER_PREFIX = 'db.query.parameter.';

    public const string DB_QUERY_TEXT = 'db.query.text';

    public const string DB_RESPONSE_RETURNED_ROWS = 'db.response.returned_rows';

    public const string DB_RESPONSE_STATUS_CODE = 'db.response.status_code';

    public const string DB_SYSTEM_NAME = 'db.system.name';

    public const string DB_TRANSACTION_NESTING_LEVEL = 'db.transaction.nesting_level';

    public const string ERROR_TYPE = 'error.type';

    public const string METRIC_OPERATION_DURATION = 'db.client.operation.duration';

    public const string METRIC_RETURNED_ROWS = 'db.client.response.returned_rows';

    public const string SERVER_ADDRESS = 'server.address';

    public const string SERVER_PORT = 'server.port';
}
