<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use DateTimeImmutable;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\TestCase;

final class AlwaysOnSamplerTest extends TestCase
{
    public function test_should_sample_always_samples_regardless_of_input(): void
    {
        $sampler = new AlwaysOnSampler();

        for ($i = 0; $i < 10; $i++) {
            $span = $this->createSpan("span-{$i}");
            $result = $sampler->shouldSample($span);

            static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
        }
    }

    public function test_should_sample_returns_record_and_sample(): void
    {
        $sampler = new AlwaysOnSampler();

        $span = $this->createSpan('test-span');
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_to_string_returns_correct_string(): void
    {
        $sampler = new AlwaysOnSampler();

        static::assertSame('AlwaysOnSampler', (string) $sampler);
    }

    private function createSpan(string $name): Span
    {
        return new Span(
            $name,
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
