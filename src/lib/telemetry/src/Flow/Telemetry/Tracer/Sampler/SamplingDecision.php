<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

enum SamplingDecision: string
{
    case DROP = 'drop';

    case RECORD_AND_SAMPLE = 'record_and_sample';

    case RECORD_ONLY = 'record_only';

    /**
     * Check if this decision indicates recording.
     */
    public function isRecording(): bool
    {
        return $this !== self::DROP;
    }

    /**
     * Check if this decision indicates sampling (export).
     */
    public function isSampled(): bool
    {
        return $this === self::RECORD_AND_SAMPLE;
    }
}
