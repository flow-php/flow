<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Sampler\TraceIdRatioBasedSampler;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TraceIdRatioBasedSamplerTest extends TestCase
{
    public static function provideInvalidRatios(): \Generator
    {
        yield 'negative' => [-0.1];
        yield 'more than 100%' => [1.1];
        yield 'way too high' => [10.0];
    }

    #[DataProvider('provideInvalidRatios')]
    public function test_constructor_throws_on_invalid_ratio(float $ratio): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Sampling ratio must be between 0.0 and 1.0');

        new TraceIdRatioBasedSampler($ratio);
    }

    public function test_deterministic_sampling_for_same_trace_id(): void
    {
        $sampler = new TraceIdRatioBasedSampler(0.5);
        $traceId = TraceId::generate();

        $firstSpan = $this->createSpan('test-span', $traceId);
        $firstResult = $sampler->shouldSample($firstSpan);

        for ($i = 0; $i < 10; $i++) {
            $span = $this->createSpan('test-span', $traceId);
            $result = $sampler->shouldSample($span);

            static::assertSame(
                $firstResult->decision,
                $result->decision,
                'Same trace ID should always produce same decision',
            );
        }
    }

    public function test_ratio_one_always_samples(): void
    {
        $sampler = new TraceIdRatioBasedSampler(1.0);

        for ($i = 0; $i < 100; $i++) {
            $span = $this->createSpan('test-span');
            $result = $sampler->shouldSample($span);

            static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
        }
    }

    public function test_ratio_produces_expected_sample_rate(): void
    {
        $sampler = new TraceIdRatioBasedSampler(0.5);
        $sampled = 0;
        $total = 1000;

        for ($i = 0; $i < $total; $i++) {
            $span = $this->createSpan('test-span');
            $result = $sampler->shouldSample($span);

            if ($result->decision === SamplingDecision::RECORD_AND_SAMPLE) {
                $sampled++;
            }
        }

        $rate = $sampled / $total;

        static::assertGreaterThan(0.4, $rate, 'Sample rate should be approximately 50%');
        static::assertLessThan(0.6, $rate, 'Sample rate should be approximately 50%');
    }

    public function test_ratio_zero_always_drops(): void
    {
        $sampler = new TraceIdRatioBasedSampler(0.0);

        for ($i = 0; $i < 100; $i++) {
            $span = $this->createSpan('test-span');
            $result = $sampler->shouldSample($span);

            static::assertSame(SamplingDecision::DROP, $result->decision);
        }
    }

    public function test_to_string_includes_ratio(): void
    {
        $sampler = new TraceIdRatioBasedSampler(0.1);

        static::assertStringContainsString('0.1', (string) $sampler);
        static::assertStringContainsString('TraceIdRatioBasedSampler', (string) $sampler);
    }

    private function createSpan(string $name, ?TraceId $traceId = null): Span
    {
        return new Span(
            $name,
            SpanContext::create($traceId ?? TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
