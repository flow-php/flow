<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

/**
 * Which links a consumed message's "process" span carries.
 */
enum MessengerHandlerLink: string
{
    /**
     * Link only to the producing (dispatcher) span the message was published from.
     */
    case Dispatcher = 'dispatcher';

    /**
     * Link only to the worker receive-cycle span the message was claimed in.
     */
    case Worker = 'worker';

    /**
     * Link to both the dispatcher and the worker receive-cycle span.
     */
    case Both = 'both';

    public function linksDispatcher(): bool
    {
        return $this === self::Dispatcher || $this === self::Both;
    }

    public function linksWorker(): bool
    {
        return $this === self::Worker || $this === self::Both;
    }
}
