<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Closure;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

/**
 * Filters spans by their attributes.
 *
 * Wraps another SpanProcessor and forwards only the spans that survive the
 * configured {@see AttributeFilter}. The decision is made at {@see self::onEnd()}
 * because span attributes are only fully populated once the span has ended;
 * {@see self::onStart()} is always delegated unchanged.
 */
final readonly class AttributeFilteringSpanProcessor implements SpanProcessor
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
        private SpanProcessor $processor,
        AttributeFilter $filter,
    ) {
        $this->shouldDrop = $filter->dropFunction();
        $this->sources = $filter->sources();
    }

    public function flush(): bool
    {
        return $this->processor->flush();
    }

    public function onEnd(Span $span): void
    {
        if (!($this->shouldDrop)(...$this->attributesFor($span))) {
            $this->processor->onEnd($span);
        }
    }

    public function onStart(Span $span): void
    {
        $this->processor->onStart($span);
    }

    public function shutdown(): void
    {
        $this->processor->shutdown();
    }

    /**
     * @return non-empty-list<Attributes>
     */
    private function attributesFor(Span $span): array
    {
        return AttributeSource::select(
            $this->sources,
            $span->attributesObject(),
            $span->resource()->attributes,
            $span->scope()->attributes,
        );
    }
}
