<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tracer\Span;

/**
 * Sampler that never records or exports any spans.
 *
 * This sampler is useful for completely disabling tracing in production
 * or for specific paths that should never be traced.
 *
 * Example usage:
 * ```php
 * $sampler = new AlwaysOffSampler();
 * $result = $sampler->shouldSample($context, $span);
 * // Always returns DROP
 * ```
 */
final readonly class AlwaysOffSampler implements Sampler
{
    public function __toString(): string
    {
        return 'AlwaysOffSampler';
    }

    public function shouldSample(Context $parentContext, Span $span): SamplingResult
    {
        return SamplingResult::drop();
    }
}
