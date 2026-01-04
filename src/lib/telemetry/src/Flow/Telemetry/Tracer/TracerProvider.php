<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\{Attributes, InstrumentationScope, Resource};
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Tracer\Sampler\{AlwaysOnSampler, Sampler};
use Psr\Clock\ClockInterface;

/**
 * Entry point for the tracing API.
 *
 * Creates and manages tracers for different instrumentation scopes.
 * The behavior (void, memory, OTLP, etc.) is determined by the injected processor.
 *
 * Example usage:
 * ```php
 * // For testing with memory storage
 * $processor = new MemoryProcessor($exporter, $exporter, $exporter);
 * $provider = new TracerProvider($processor, new SystemClock());
 *
 * // For OTLP export
 * $processor = batching_span_processor(otlp_span_exporter($transport));
 * $provider = new TracerProvider($processor, new SystemClock());
 *
 * // For void/disabled tracing
 * $provider = new TracerProvider(new VoidProcessor(), new SystemClock());
 *
 * $tracer = $provider->tracer('my-service', '1.0.0');
 * $span = $tracer->span('operation');
 * $tracer->complete($span);
 * ```
 */
final readonly class TracerProvider
{
    public function __construct(
        private SpanProcessor $processor,
        private ClockInterface $clock,
        private ContextStorage $contextStorage,
        private Sampler $sampler = new AlwaysOnSampler(),
    ) {
    }

    /**
     * Create a tracer for the given instrumentation scope.
     *
     * Note: Instance caching is handled by the Telemetry class.
     * This method always creates a new Tracer instance.
     *
     * @param resource $resource The resource context for all spans from this tracer
     * @param string $name The name of the instrumentation scope (e.g., library name)
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function tracer(Resource $resource, string $name, string $version = 'unknown', ?string $schemaUrl = null, ?Attributes $attributes = null) : Tracer
    {
        return new Tracer(
            $resource,
            new InstrumentationScope($name, $version, $schemaUrl, $attributes ?? new Attributes()),
            $this->processor,
            $this->clock,
            $this->contextStorage,
            $this->sampler,
        );
    }
}
