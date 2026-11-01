<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * @see https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
 */
enum SpanKind: string
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
