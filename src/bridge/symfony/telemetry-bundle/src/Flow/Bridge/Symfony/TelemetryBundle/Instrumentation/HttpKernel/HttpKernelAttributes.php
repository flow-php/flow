<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

/**
 * Custom keys only, under `flow.symfony.`; official keys live in SemConvAttributes.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class HttpKernelAttributes
{
    public const string ATTR_CONTROLLER = 'flow.symfony.controller';

    public const string ATTR_CONTROLLER_ARGUMENT = 'flow.symfony.controller.argument';
}
