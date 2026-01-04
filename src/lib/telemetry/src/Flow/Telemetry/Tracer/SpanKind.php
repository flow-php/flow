<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * Describes the relationship between a span and its parent/children.
 *
 * SpanKind defines the type of operation represented by a span according
 * to OpenTelemetry specification. It helps in understanding the role of
 * the span within a distributed system.
 *
 * @see https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
 */
enum SpanKind : string
{
    /**
     * Client-side of a synchronous RPC or HTTP request.
     */
    case CLIENT = 'client';

    /**
     * Consumer of an asynchronous message (e.g., message queue).
     */
    case CONSUMER = 'consumer';
    /**
     * Default kind for internal operations that don't cross process boundaries.
     */
    case INTERNAL = 'internal';

    /**
     * Producer of an asynchronous message (e.g., message queue).
     */
    case PRODUCER = 'producer';

    /**
     * Server-side handling of a synchronous RPC or HTTP request.
     */
    case SERVER = 'server';
}
