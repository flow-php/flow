<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\TraceState;

/**
 * Result of a sampling decision.
 *
 * Contains the sampling decision along with additional attributes
 * that should be added to the span and an optional updated trace state.
 *
 * Example usage:
 * ```php
 * $result = new SamplingResult(
 *     SamplingDecision::RECORD_AND_SAMPLE,
 *     ['sampling.reason' => 'head-based'],
 * );
 *
 * if ($result->decision->isSampled()) {
 *     // Create and export the span
 * }
 * ```
 */
final readonly class SamplingResult
{
    /**
     * @var array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public array $attributes;

    /**
     * @param SamplingDecision $decision The sampling decision
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Additional span attributes from the sampler
     * @param null|TraceState $traceState Updated trace state, or null to keep existing
     */
    public function __construct(
        public SamplingDecision $decision,
        array|Attributes $attributes = [],
        public ?TraceState $traceState = null,
    ) {
        $this->attributes = $attributes instanceof Attributes ? $attributes->normalize() : $attributes;
    }

    /**
     * Create a result indicating the span should be dropped.
     */
    public static function drop(): self
    {
        return new self(SamplingDecision::DROP);
    }

    /**
     * Create a result indicating the span should be recorded and exported.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes
     */
    public static function recordAndSample(array|Attributes $attributes = [], ?TraceState $traceState = null): self
    {
        return new self(SamplingDecision::RECORD_AND_SAMPLE, $attributes, $traceState);
    }

    /**
     * Create a result indicating the span should be recorded but not exported.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes
     */
    public static function recordOnly(array|Attributes $attributes = [], ?TraceState $traceState = null): self
    {
        return new self(SamplingDecision::RECORD_ONLY, $attributes, $traceState);
    }
}
