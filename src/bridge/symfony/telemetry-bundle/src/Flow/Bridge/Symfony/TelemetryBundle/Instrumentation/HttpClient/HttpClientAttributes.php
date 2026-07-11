<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

/**
 * Flow-specific HTTP client attribute keys.
 *
 * Official HTTP semantic convention keys come from {@see \Flow\Telemetry\SemConvAttributes};
 * per OTel naming guidance flow-custom keys must not extend the `http.` namespace and live under
 * the `flow.http.` prefix instead.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class HttpClientAttributes
{
    public const string ATTR_CLIENT_NAME = 'flow.http.client.name';
}
