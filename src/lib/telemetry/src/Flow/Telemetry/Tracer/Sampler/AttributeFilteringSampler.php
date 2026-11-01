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
 * Sees start-time attributes only; end-state filtering is AttributeFilteringSpanProcessor's job.
 */
final readonly class AttributeFilteringSampler implements Sampler
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
        return 'AttributeFilteringSampler';
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
