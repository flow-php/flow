<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

/**
 * Flow-specific HttpKernel attribute keys.
 *
 * Official HTTP semantic convention keys come from {@see \Flow\Telemetry\SemConvAttributes};
 * per OTel naming guidance flow-custom keys live under the `flow.symfony.` prefix.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class HttpKernelAttributes
{
    public const string ATTR_CONTROLLER = 'flow.symfony.controller';

    public const string ATTR_CONTROLLER_ARGUMENT = 'flow.symfony.controller.argument';
}
