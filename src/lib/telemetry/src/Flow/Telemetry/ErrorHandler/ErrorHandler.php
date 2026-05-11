<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Receives Throwables raised inside the SDK at runtime.
 *
 * Per the OpenTelemetry specification the SDK MUST NOT throw to user code at
 * runtime touchpoints. Components catch \Throwable from exporters/processors and
 * forward it here. Implementations MUST swallow any failure of their own (a
 * handler that itself throws is considered broken).
 *
 * @see https://opentelemetry.io/docs/specs/otel/error-handling/
 */
interface ErrorHandler
{
    public function handle(\Throwable $error): void;
}
