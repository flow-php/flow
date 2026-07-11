<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\Sampler\AlwaysOffSampler;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use PHPUnit\Framework\TestCase;

final class SuppressingSamplerTest extends TestCase
{
    public function test_drops_span_when_parent_context_is_suppressed(): void
    {
        $sampler = new SuppressingSampler(new AlwaysOnSampler());

        static::assertSame(
            SamplingDecision::DROP,
            $sampler->shouldSample(Context::root()->withSuppressedTracing(), SpanMother::create())->decision,
        );
    }

    public function test_delegates_to_inner_sampler_when_not_suppressed(): void
    {
        $recording = new SuppressingSampler(new AlwaysOnSampler());
        $dropping = new SuppressingSampler(new AlwaysOffSampler());

        static::assertSame(
            SamplingDecision::RECORD_AND_SAMPLE,
            $recording->shouldSample(Context::root(), SpanMother::create())->decision,
        );
        static::assertSame(
            SamplingDecision::DROP,
            $dropping->shouldSample(Context::root(), SpanMother::create())->decision,
        );
    }

    public function test_string_representation_wraps_inner(): void
    {
        static::assertSame('Suppressing{AlwaysOnSampler}', (string) new SuppressingSampler(new AlwaysOnSampler()));
    }
}
