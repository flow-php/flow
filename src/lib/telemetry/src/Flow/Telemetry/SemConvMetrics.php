<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Official OpenTelemetry semantic convention metric names emitted by Flow instrumentations.
 *
 * Flow-specific metrics use the `flow.` prefix and are named in the packages that emit them.
 */
final class SemConvMetrics
{
    /**
     * @see https://opentelemetry.io/docs/specs/semconv/database/database-metrics/
     */
    public const string DB_CLIENT_OPERATION_DURATION = 'db.client.operation.duration';

    public const string DB_CLIENT_RESPONSE_RETURNED_ROWS = 'db.client.response.returned_rows';

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
     */
    public const string MESSAGING_CLIENT_CONSUMED_MESSAGES = 'messaging.client.consumed.messages';

    public const string MESSAGING_CLIENT_SENT_MESSAGES = 'messaging.client.sent.messages';

    public const string MESSAGING_PROCESS_DURATION = 'messaging.process.duration';
}
