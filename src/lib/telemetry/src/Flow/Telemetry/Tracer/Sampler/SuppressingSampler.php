<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tracer\Span;

use function sprintf;

/**
 * The single tracing-suppression enforcement point, wrapping any sampler.
 */
final readonly class SuppressingSampler implements Sampler
{
    public function __construct(
        private Sampler $inner,
    ) {}

    public function __toString(): string
    {
        return sprintf('Suppressing{%s}', (string) $this->inner);
    }

    public function shouldSample(Context $parentContext, Span $span): SamplingResult
    {
        if ($parentContext->isTracingSuppressed()) {
            return SamplingResult::drop();
        }

        return $this->inner->shouldSample($parentContext, $span);
    }
}
