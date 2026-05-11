<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Tracer\Span;

/**
 * Sampler that always records and exports all spans.
 *
 * This is the default sampler and should be used during development
 * or when full observability is required regardless of cost.
 *
 * Example usage:
 * ```php
 * $sampler = new AlwaysOnSampler();
 * $result = $sampler->shouldSample($span);
 * // Always returns RECORD_AND_SAMPLE
 * ```
 */
final readonly class AlwaysOnSampler implements Sampler
{
    public function __toString(): string
    {
        return 'AlwaysOnSampler';
    }

    public function shouldSample(Span $span): SamplingResult
    {
        return SamplingResult::recordAndSample();
    }
}
