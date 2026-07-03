<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

/**
 * Flow-specific messenger attribute keys.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class MessengerAttributes
{
    public const string ATTR_BUS = 'flow.messenger.bus';

    public const string ATTR_MESSAGE_CLASS = 'flow.messenger.message.class';
}
