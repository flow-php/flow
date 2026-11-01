<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

/**
 * Custom keys only, under `flow.http.`; official keys live in SemConvAttributes.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class HttpClientAttributes
{
    public const string ATTR_CLIENT_NAME = 'flow.http.client.name';
}
