<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

/**
 * Custom keys only, under `flow.symfony.`; official keys live in SemConvAttributes.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class ConsoleAttributes
{
    public const string ATTR_COMMAND_CLASS = 'flow.symfony.command.class';

    public const string ATTR_COMMAND_NAME = 'flow.symfony.command.name';

    public const string ATTR_COMMAND_SIGNAL = 'flow.symfony.command.signal';
}
