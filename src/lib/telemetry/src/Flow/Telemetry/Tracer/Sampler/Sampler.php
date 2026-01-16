<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Tracer\Span;

/**
 * Interface for sampling decisions.
 *
 * Samplers determine whether a span should be recorded and/or exported.
 * This allows controlling the volume of telemetry data while maintaining
 * the ability to trace complete transactions.
 *
 * Example usage:
 * ```php
 * $sampler = new TraceIdRatioBasedSampler(0.1); // 10% sampling
 *
 * $result = $sampler->shouldSample($span);
 *
 * if ($result->decision->isRecording()) {
 *     // Record the span
 * }
 * ```
 */
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
    public function __toString() : string;

    /**
     * Determine if a span should be sampled.
     *
     * @param Span $span The span to evaluate for sampling
     *
     * @return SamplingResult The sampling decision
     */
    public function shouldSample(Span $span) : SamplingResult;
}
