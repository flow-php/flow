<?php

declare(strict_types=1);

namespace Flow\Telemetry\Exporter;

use Flow\Telemetry\Signal\Signals;

/**
 * Interface for exporting telemetry signals to external systems.
 *
 * Exporters are responsible for transmitting log, metric, and span batches
 * to backends like OTLP collectors, console output, or in-memory stores.
 *
 * The signal type is carried on the {@see Signal::$type} discriminator.
 */
interface Exporter
{
    /**
     * Export a signal batch.
     *
     * Implementations dispatch on {@see Signal::$type} via match.
     *
     * @return bool True on success, false on failure
     */
    public function export(Signals $signal) : bool;

    /**
     * Shutdown the exporter and any owned transports.
     *
     * Implementations MUST be idempotent and MUST NOT throw - runtime errors
     * are routed through the configured error handler instead.
     */
    public function shutdown() : void;
}
