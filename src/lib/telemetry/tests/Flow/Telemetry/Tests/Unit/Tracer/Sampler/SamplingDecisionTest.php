<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use PHPUnit\Framework\TestCase;

final class SamplingDecisionTest extends TestCase
{
    public function test_drop_is_not_recording() : void
    {
        self::assertFalse(SamplingDecision::DROP->isRecording());
    }

    public function test_drop_is_not_sampled() : void
    {
        self::assertFalse(SamplingDecision::DROP->isSampled());
    }

    public function test_record_and_sample_is_recording() : void
    {
        self::assertTrue(SamplingDecision::RECORD_AND_SAMPLE->isRecording());
    }

    public function test_record_and_sample_is_sampled() : void
    {
        self::assertTrue(SamplingDecision::RECORD_AND_SAMPLE->isSampled());
    }

    public function test_record_only_is_not_sampled() : void
    {
        self::assertFalse(SamplingDecision::RECORD_ONLY->isSampled());
    }

    public function test_record_only_is_recording() : void
    {
        self::assertTrue(SamplingDecision::RECORD_ONLY->isRecording());
    }
}
