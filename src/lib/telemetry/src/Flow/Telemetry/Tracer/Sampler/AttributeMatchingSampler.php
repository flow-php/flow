<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Closure;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Tracer\Span;

/**
 * Drops spans whose start-time attributes match an {@see AttributeFilter}, and
 * defers every other span to a delegate sampler.
 *
 * The decision is made at span start, so a matched span becomes non-recording and
 * is never handed to a processor or exporter (no attributes accumulate, no onEnd).
 *
 * Only attributes available at span start are visible here - attributes added
 * during the span's lifetime (status codes, durations) are not. End-state
 * filtering remains the job of a span processor or tail sampling.
 *
 * The matcher tree, its compiled drop closure, the inspected
 * {@see AttributeSource}s and the exclude polarity are all reused from the shared
 * {@see AttributeFilter}: a match drops the span ({@see AttributeFilter::$exclude}
 * true, the default) or keeps ONLY matching spans (exclude false).
 */
final readonly class AttributeMatchingSampler implements Sampler
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
        AttributeFilter $filter,
        private Sampler $delegate = new AlwaysOnSampler(),
    ) {
        $this->shouldDrop = $filter->dropFunction();
        $this->sources = $filter->sources();
    }

    public function __toString(): string
    {
        return 'AttributeMatchingSampler';
    }

    public function shouldSample(Context $parentContext, Span $span): SamplingResult
    {
        if (($this->shouldDrop)(...$this->attributesFor($span))) {
            return SamplingResult::drop();
        }

        return $this->delegate->shouldSample($parentContext, $span);
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
