<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

/**
 * Flow-specific console attribute keys.
 *
 * Official keys (e.g. `process.exit.code`, `error.type`) come from
 * {@see \Flow\Telemetry\SemConvAttributes}; per OTel naming guidance flow-custom keys live under
 * the `flow.symfony.` prefix. The signal attribute has no registry home, hence the flow key.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class ConsoleAttributes
{
    public const string ATTR_COMMAND_CLASS = 'flow.symfony.command.class';

    public const string ATTR_COMMAND_NAME = 'flow.symfony.command.name';

    public const string ATTR_COMMAND_SIGNAL = 'flow.symfony.command.signal';
}
