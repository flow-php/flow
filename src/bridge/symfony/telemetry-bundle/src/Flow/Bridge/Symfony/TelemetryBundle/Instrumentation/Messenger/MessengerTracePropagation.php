<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

/**
 * How a consumed message's span relates to the producing (publishing) span when
 * trace context is propagated across the Messenger transport.
 */
enum MessengerTracePropagation: string
{
    /**
     * The consumer span adopts the producer's trace and becomes its child, so
     * publish -> queue -> consume is one continuous distributed trace.
     */
    case Continuation = 'continue';

    /**
     * The consumer span stays in the worker's own trace and carries a span link
     * back to the producer span. Producer and consumer get separate, clean traces
     * connected by a link (recommended for decoupled / batch / long-delay queues).
     */
    case Link = 'link';
}
