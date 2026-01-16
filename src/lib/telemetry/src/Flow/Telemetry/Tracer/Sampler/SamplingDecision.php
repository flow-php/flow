<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

/**
 * Sampling decision made by a Sampler.
 *
 * DROP: The span should not be recorded and will not be exported.
 *       isRecording() returns false.
 *
 * RECORD_ONLY: The span should be recorded but not exported.
 *              Useful for local debugging or custom processing.
 *              isRecording() returns true, but the SAMPLED flag is not set.
 *
 * RECORD_AND_SAMPLE: The span should be recorded and exported.
 *                    isRecording() returns true and SAMPLED flag is set.
 */
enum SamplingDecision : string
{
    case DROP = 'drop';

    case RECORD_AND_SAMPLE = 'record_and_sample';

    case RECORD_ONLY = 'record_only';

    /**
     * Check if this decision indicates recording.
     */
    public function isRecording() : bool
    {
        return $this !== self::DROP;
    }

    /**
     * Check if this decision indicates sampling (export).
     */
    public function isSampled() : bool
    {
        return $this === self::RECORD_AND_SAMPLE;
    }
}
