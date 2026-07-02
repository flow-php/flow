<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tracer\Span;

use function sprintf;

/**
 * Enforces OpenTelemetry tracing suppression as a Sampler: when the parent context carries the
 * suppression bit, the span is dropped (non-recording, never exported); otherwise the decision is
 * deferred to the wrapped sampler.
 *
 * This is the single enforcement point for the context-scoped suppression key, composed over whatever
 * sampler is configured, so suppression takes precedence over any sampling strategy.
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
