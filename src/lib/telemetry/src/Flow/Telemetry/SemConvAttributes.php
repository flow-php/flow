<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Official OpenTelemetry semantic convention attribute keys emitted by Flow instrumentations.
 *
 * Single source of truth for every official key used across the monorepo — flow-specific custom
 * keys live in per-package attribute classes and must use the `flow.` prefix instead of extending
 * any of these namespaces.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class SemConvAttributes
{
    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/
     */
    public const string CODE_FUNCTION_NAME = 'code.function.name';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/database/database-spans/
     */
    public const string DB_COLLECTION_NAME = 'db.collection.name';

    public const string DB_NAMESPACE = 'db.namespace';

    public const string DB_OPERATION_NAME = 'db.operation.name';

    public const string DB_QUERY_PARAMETER_PREFIX = 'db.query.parameter.';

    public const string DB_QUERY_TEXT = 'db.query.text';

    public const string DB_RESPONSE_RETURNED_ROWS = 'db.response.returned_rows';

    public const string DB_RESPONSE_STATUS_CODE = 'db.response.status_code';

    public const string DB_SYSTEM_NAME = 'db.system.name';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/
     */
    public const string ERROR_TYPE = 'error.type';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/http/http-spans/
     */
    public const string HTTP_REQUEST_METHOD = 'http.request.method';

    public const string HTTP_RESPONSE_STATUS_CODE = 'http.response.status_code';

    public const string HTTP_ROUTE = 'http.route';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
     */
    public const string MESSAGING_CONSUMER_GROUP_NAME = 'messaging.consumer.group.name';

    public const string MESSAGING_DESTINATION_NAME = 'messaging.destination.name';

    public const string MESSAGING_MESSAGE_ID = 'messaging.message.id';

    public const string MESSAGING_OPERATION_NAME = 'messaging.operation.name';

    public const string MESSAGING_OPERATION_TYPE = 'messaging.operation.type';

    public const string MESSAGING_SYSTEM = 'messaging.system';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/process/
     */
    public const string PROCESS_EXIT_CODE = 'process.exit.code';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/server/
     */
    public const string SERVER_ADDRESS = 'server.address';

    public const string SERVER_PORT = 'server.port';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/test/
     */
    public const string TEST_CASE_NAME = 'test.case.name';

    public const string TEST_CASE_RESULT_STATUS = 'test.case.result.status';

    public const string TEST_SUITE_NAME = 'test.suite.name';

    public const string TEST_SUITE_RUN_STATUS = 'test.suite.run.status';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/url/
     */
    public const string URL_FULL = 'url.full';

    public const string URL_PATH = 'url.path';

    public const string URL_QUERY = 'url.query';

    public const string URL_SCHEME = 'url.scheme';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/user-agent/
     */
    public const string USER_AGENT_ORIGINAL = 'user_agent.original';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/user/
     */
    public const string USER_EMAIL = 'user.email';

    public const string USER_ID = 'user.id';

    public const string USER_ROLES = 'user.roles';
}
