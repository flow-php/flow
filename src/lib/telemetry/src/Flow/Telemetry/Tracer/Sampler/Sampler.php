<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tracer\Span;

interface Sampler
{
    /**
     * Get a string representation of this sampler for debugging.
     *
     * Examples:
     * - "AlwaysOnSampler"
     * - "TraceIdRatioBasedSampler{0.001}"
     * - "ParentBased{root=AlwaysOnSampler}"
     */
    public function __toString(): string;

    /**
     * Determine if a span should be sampled.
     *
     * @param Context $parentContext The parent context of the span to be created
     * @param Span $span The span to evaluate for sampling
     *
     * @return SamplingResult The sampling decision
     */
    public function shouldSample(Context $parentContext, Span $span): SamplingResult;
}
