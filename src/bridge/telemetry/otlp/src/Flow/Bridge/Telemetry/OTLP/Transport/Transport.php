<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Telemetry\Signal\Signals;

interface Transport
{
    /**
     * Send a signal batch to the backend.
     *
     * Implementations dispatch on {@see Signal::$type} via match.
     *
     * @throws TransportException On transport failure
     */
    public function send(Signals $signal): void;

    /**
     * Shutdown the transport, releasing any resources.
     *
     * Should complete any pending sends before shutting down.
     * This method is idempotent - calling it multiple times is safe.
     */
    public function shutdown(): void;
}
