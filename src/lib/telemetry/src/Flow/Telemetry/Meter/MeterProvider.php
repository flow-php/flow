<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\Exemplar\ExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\TraceBasedExemplarFilter;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Resource;
use Psr\Clock\ClockInterface;

final readonly class MeterProvider
{
    public function __construct(
        private MetricProcessor $processor,
        private ClockInterface $clock = new SystemClock(),
        private AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
        private ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
        private MetricLimits $limits = new MetricLimits(),
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    /**
     * Create a meter for the given instrumentation scope.
     *
     * Note: Instance caching is handled by the Telemetry class.
     * This method always creates a new Meter instance.
     *
     * @param \Flow\Telemetry\Resource $resource The resource context for all metrics from this meter
     * @param string $name The name of the instrumentation scope (e.g., library name)
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     * @param null|Attributes $signalAttributes Default attributes merged into every recorded metric data point (per-call values win)
     */
    public function meter(
        Resource $resource,
        string $name,
        string $version = 'unknown',
        ?string $schemaUrl = null,
        ?Attributes $attributes = null,
        ?Attributes $signalAttributes = null,
    ): Meter {
        return new Meter(
            $resource,
            new InstrumentationScope($name, $version, $schemaUrl, $attributes ?? new Attributes()),
            $this->processor,
            $this->clock,
            $this->temporality,
            $this->exemplarFilter,
            $this->limits,
            $this->errorHandler,
            $signalAttributes ?? new Attributes(),
        );
    }
}
