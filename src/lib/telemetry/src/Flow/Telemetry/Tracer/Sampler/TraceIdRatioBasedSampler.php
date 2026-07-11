<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\Span;
use InvalidArgumentException;

use function ord;
use function sprintf;
use function substr;

/**
 * Sampler that samples a configurable percentage of traces.
 *
 * The sampling decision is deterministic based on the trace ID,
 * ensuring that all spans in a trace are sampled consistently
 * across different services.
 *
 * Example usage:
 * ```php
 * // Sample 10% of traces
 * $sampler = new TraceIdRatioBasedSampler(0.1);
 * $result = $sampler->shouldSample($context, $span);
 * ```
 */
final readonly class TraceIdRatioBasedSampler implements Sampler
{
    private int $threshold;

    /**
     * @param float $ratio Sampling ratio between 0.0 (none) and 1.0 (all)
     *
     * @throws \InvalidArgumentException if ratio is out of range
     */
    public function __construct(
        private float $ratio,
    ) {
        if ($ratio < 0.0 || $ratio > 1.0) {
            throw new InvalidArgumentException(sprintf('Sampling ratio must be between 0.0 and 1.0, got %f', $ratio));
        }

        $this->threshold = $ratio >= 1.0 ? PHP_INT_MAX : (int) ($ratio * PHP_INT_MAX);
    }

    public function __toString(): string
    {
        return sprintf('TraceIdRatioBasedSampler{%.6f}', $this->ratio);
    }

    public function shouldSample(Context $parentContext, Span $span): SamplingResult
    {
        if ($this->ratio >= 1.0) {
            return SamplingResult::recordAndSample();
        }

        if ($this->ratio <= 0.0) {
            return SamplingResult::drop();
        }

        $traceIdValue = $this->traceIdToInt($span->context()->traceId);

        if ($traceIdValue < $this->threshold) {
            return SamplingResult::recordAndSample();
        }

        return SamplingResult::drop();
    }

    /**
     * Convert trace ID bytes to a deterministic integer for comparison.
     *
     * Uses the lower 8 bytes of the trace ID for the comparison.
     */
    private function traceIdToInt(TraceId $traceId): int
    {
        $bytes = $traceId->toBytes();
        $lowerBytes = substr($bytes, 8, 8);

        $value = 0;

        for ($i = 0; $i < 8; $i++) {
            $value = ($value << 8) | ord($lowerBytes[$i]);
        }

        return $value & PHP_INT_MAX;
    }
}
