<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\TraceState;

/**
 * @import-type TAttributeValueMap from Attributes
 */
final readonly class SamplingResult
{
    /**
     * @var TAttributeValueMap
     */
    public array $attributes;

    /**
     * @param SamplingDecision $decision The sampling decision
     * @param TAttributeValueMap|Attributes $attributes Additional span attributes from the sampler
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
     * @param TAttributeValueMap|Attributes $attributes
     */
    public static function recordAndSample(array|Attributes $attributes = [], ?TraceState $traceState = null): self
    {
        return new self(SamplingDecision::RECORD_AND_SAMPLE, $attributes, $traceState);
    }

    /**
     * Create a result indicating the span should be recorded but not exported.
     *
     * @param TAttributeValueMap|Attributes $attributes
     */
    public static function recordOnly(array|Attributes $attributes = [], ?TraceState $traceState = null): self
    {
        return new self(SamplingDecision::RECORD_ONLY, $attributes, $traceState);
    }
}
