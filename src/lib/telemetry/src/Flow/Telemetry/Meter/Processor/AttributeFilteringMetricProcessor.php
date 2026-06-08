<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Closure;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;

/**
 * Filters metrics by their attributes.
 *
 * Wraps another MetricProcessor and forwards only the metrics that survive the
 * configured {@see AttributeFilter}. Metrics the filter drops are silently
 * discarded to reduce telemetry noise.
 */
final readonly class AttributeFilteringMetricProcessor implements MetricProcessor
{
    /**
     * @var Closure(Attributes ...): bool
     */
    private Closure $shouldDrop;

    /**
     * @var non-empty-list<AttributeSource>
     */
    private array $sources;

    public function __construct(
        private MetricProcessor $processor,
        AttributeFilter $filter,
    ) {
        $this->shouldDrop = $filter->dropFunction();
        $this->sources = $filter->sources();
    }

    public function flush(): bool
    {
        return $this->processor->flush();
    }

    public function process(Metric $metric): void
    {
        if (!($this->shouldDrop)(...$this->attributesFor($metric))) {
            $this->processor->process($metric);
        }
    }

    public function shutdown(): void
    {
        $this->processor->shutdown();
    }

    /**
     * @return non-empty-list<Attributes>
     */
    private function attributesFor(Metric $metric): array
    {
        return AttributeSource::select(
            $this->sources,
            $metric->attributes,
            $metric->resource->attributes,
            $metric->scope->attributes,
        );
    }
}
