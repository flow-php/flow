<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use Flow\Telemetry\Logger\{Logger, LoggerProvider};
use Flow\Telemetry\Meter\{Meter, MeterProvider};
use Flow\Telemetry\Tracer\{Tracer, TracerProvider};
use Flow\Telemetry\Transport\Transport;

/**
 * Main entry point to all telemetry operations.
 *
 * Provides unified access to tracing, metrics, and logging through
 * their respective providers. Coordinates lifecycle operations
 * (flush, shutdown) across all providers.
 *
 * Instances of Tracer, Meter, and Logger are cached by this class
 * to ensure that calling tracer/meter/logger with the same parameters
 * returns the same instance.
 */
final class Telemetry
{
    /**
     * @var array<string, Logger>
     */
    private array $loggers = [];

    /**
     * @var array<string, Meter>
     */
    private array $meters = [];

    /**
     * @var array<string, Tracer>
     */
    private array $tracers = [];

    public function __construct(
        private readonly Resource $resource,
        private readonly TracerProvider $tracerProvider,
        private readonly MeterProvider $meterProvider,
        private readonly LoggerProvider $loggerProvider,
    ) {
    }

    /**
     * Flush all pending telemetry data.
     *
     * Flushes all cached tracers, meters, and loggers.
     * Returns true only if all flush operations succeed.
     */
    public function flush() : bool
    {
        $success = true;

        foreach ($this->tracers as $tracer) {
            $success = $tracer->flush() && $success;
        }

        foreach ($this->meters as $meter) {
            $success = $meter->flush() && $success;
        }

        foreach ($this->loggers as $logger) {
            $success = $logger->flush() && $success;
        }

        return $success;
    }

    /**
     * Get or create a logger for the given scope.
     *
     * Loggers are cached by name, version, schemaUrl, and attributes.
     * Calling this method with the same parameters returns the same Logger instance.
     *
     * @param string $name The name of the instrumentation scope
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function logger(string $name, string $version = 'unknown', ?string $schemaUrl = null, ?Attributes $attributes = null) : Logger
    {
        $key = $name . '@' . $version . '@' . ($schemaUrl ?? '') . '@' . ($attributes?->id() ?? '');

        if (!\array_key_exists($key, $this->loggers)) {
            $this->loggers[$key] = $this->loggerProvider->logger($this->resource, $name, $version, $schemaUrl, $attributes);
        }

        return $this->loggers[$key];
    }

    /**
     * Get or create a meter for the given scope.
     *
     * Meters are cached by name, version, schemaUrl, and attributes.
     * Calling this method with the same parameters returns the same Meter instance.
     *
     * @param string $name The name of the instrumentation scope
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function meter(string $name, string $version = 'unknown', ?string $schemaUrl = null, ?Attributes $attributes = null) : Meter
    {
        $key = $name . '@' . $version . '@' . ($schemaUrl ?? '') . '@' . ($attributes?->id() ?? '');

        if (!\array_key_exists($key, $this->meters)) {
            $this->meters[$key] = $this->meterProvider->meter($this->resource, $name, $version, $schemaUrl, $attributes);
        }

        return $this->meters[$key];
    }

    /**
     * Register a shutdown function to automatically shutdown telemetry.
     *
     * This ensures all pending spans, metrics, and logs are exported
     * when the PHP script terminates. The shutdown function will call
     * shutdown() which flushes and releases all provider resources.
     *
     * @return $this For method chaining
     */
    public function registerShutdownFunction() : self
    {
        \register_shutdown_function(fn () => $this->shutdown());

        return $this;
    }

    /**
     * Shutdown all telemetry and release resources.
     *
     * Flushes all pending data, then shuts down all transports.
     * Transport shutdown is idempotent, allowing multiple exporters
     * to share the same transport safely.
     */
    public function shutdown() : bool
    {
        $flushed = $this->flush();

        /** @var \SplObjectStorage<Transport, true> $transports */
        $transports = new \SplObjectStorage();

        foreach ($this->tracers as $tracer) {
            foreach ($tracer->processor()->exporter()->transports() as $transport) {
                $transports->attach($transport);
            }
        }

        foreach ($this->meters as $meter) {
            foreach ($meter->processor()->exporter()->transports() as $transport) {
                $transports->attach($transport);
            }
        }

        foreach ($this->loggers as $logger) {
            foreach ($logger->processor()->exporter()->transports() as $transport) {
                $transports->attach($transport);
            }
        }

        foreach ($transports as $transport) {
            $transport->shutdown();
        }

        return $flushed;
    }

    /**
     * Get or create a tracer for the given scope.
     *
     * Tracers are cached by name, version, schemaUrl, and attributes.
     * Calling this method with the same parameters returns the same Tracer instance.
     *
     * @param string $name The name of the instrumentation scope
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function tracer(string $name, string $version = 'unknown', ?string $schemaUrl = null, ?Attributes $attributes = null) : Tracer
    {
        $key = $name . '@' . $version . '@' . ($schemaUrl ?? '') . '@' . ($attributes?->id() ?? '');

        if (!\array_key_exists($key, $this->tracers)) {
            $this->tracers[$key] = $this->tracerProvider->tracer($this->resource, $name, $version, $schemaUrl, $attributes);
        }

        return $this->tracers[$key];
    }
}
