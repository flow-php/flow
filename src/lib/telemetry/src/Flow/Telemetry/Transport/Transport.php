<?php

declare(strict_types=1);

namespace Flow\Telemetry\Transport;

use Flow\Telemetry\Signal\Signals;

/**
 * Interface for sending telemetry data to backends.
 *
 * Transports handle the actual network communication to telemetry backends
 * like OTLP collectors, Jaeger, or other observability platforms.
 *
 * The signal type (traces, metrics, logs) is carried on the {@see Signal::$type} discriminator.
 */
interface Transport
{
    /**
     * Send a signal batch to the backend.
     *
     * Implementations dispatch on {@see Signal::$type} via match.
     *
     * @throws TransportException On transport failure
     */
    public function send(Signals $signal) : void;

    /**
     * Shutdown the transport, releasing any resources.
     *
     * Should complete any pending sends before shutting down.
     * This method is idempotent - calling it multiple times is safe.
     */
    public function shutdown() : void;
}
