<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Tracer\Span;

use function sprintf;

/**
 * Sampler that respects the parent span's sampling decision.
 *
 * This sampler ensures sampling consistency across a trace:
 * - If the parent was sampled, this span is sampled
 * - If the parent was not sampled, this span is not sampled
 * - For root spans (no parent), delegates to the configured root sampler
 *
 * This is the recommended sampler for most production deployments as it
 * ensures complete traces while maintaining sampling decisions made upstream.
 *
 * Example usage:
 * ```php
 * $sampler = new ParentBasedSampler(
 *     new TraceIdRatioBasedSampler(0.01), // 1% for root spans
 * );
 * $result = $sampler->shouldSample($span);
 * ```
 */
final readonly class ParentBasedSampler implements Sampler
{
    /**
     * @param Sampler $rootSampler Sampler to use for root spans (no parent)
     * @param null|Sampler $remoteParentSampled Sampler for remote sampled parents (default: AlwaysOnSampler)
     * @param null|Sampler $remoteParentNotSampled Sampler for remote non-sampled parents (default: AlwaysOffSampler)
     * @param null|Sampler $localParentSampled Sampler for local sampled parents (default: AlwaysOnSampler)
     * @param null|Sampler $localParentNotSampled Sampler for local non-sampled parents (default: AlwaysOffSampler)
     */
    public function __construct(
        private Sampler $rootSampler,
        private ?Sampler $remoteParentSampled = null,
        private ?Sampler $remoteParentNotSampled = null,
        private ?Sampler $localParentSampled = null,
        private ?Sampler $localParentNotSampled = null,
    ) {}

    public function __toString(): string
    {
        return sprintf('ParentBased{root=%s}', (string) $this->rootSampler);
    }

    public function shouldSample(Span $span): SamplingResult
    {
        $context = $span->context();

        if ($context->parentSpanId === null) {
            return $this->rootSampler->shouldSample($span);
        }

        $sampler = $this->getSamplerForParent($context->traceFlags->isSampled(), $context->isRemote);

        return $sampler->shouldSample($span);
    }

    private function getSamplerForParent(bool $isSampled, bool $isRemote): Sampler
    {
        if ($isRemote) {
            if ($isSampled) {
                return $this->remoteParentSampled ?? new AlwaysOnSampler();
            }

            return $this->remoteParentNotSampled ?? new AlwaysOffSampler();
        }

        if ($isSampled) {
            return $this->localParentSampled ?? new AlwaysOnSampler();
        }

        return $this->localParentNotSampled ?? new AlwaysOffSampler();
    }
}
