<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * Status codes for a span according to OpenTelemetry specification.
 *
 * @see https://opentelemetry.io/docs/specs/otel/trace/api/#set-status
 */
enum SpanStatusCode: int
{
    /**
     * Operation failed with an error.
     */
    case ERROR = 2;

    /**
     * Operation completed successfully.
     */
    case OK = 1;
    /**
     * Default status, indicates the span has not been explicitly set.
     */
    case UNSET = 0;
}
