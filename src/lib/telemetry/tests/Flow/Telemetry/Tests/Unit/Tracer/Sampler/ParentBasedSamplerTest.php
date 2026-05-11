<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Sampler\AlwaysOffSampler;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\ParentBasedSampler;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\TestCase;

final class ParentBasedSamplerTest extends TestCase
{
    public function test_custom_local_parent_not_sampled_sampler(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler(), null, null, null, new AlwaysOnSampler());

        $span = $this->createSpanWithParent(TraceFlags::default(), isRemote: false);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_custom_local_parent_sampled_sampler(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler(), null, null, new AlwaysOffSampler());

        $span = $this->createSpanWithParent(TraceFlags::sampled(), isRemote: false);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::DROP, $result->decision);
    }

    public function test_custom_remote_parent_not_sampled_sampler(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler(), null, new AlwaysOnSampler());

        $span = $this->createSpanWithParent(TraceFlags::default(), isRemote: true);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_custom_remote_parent_sampled_sampler(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler(), new AlwaysOffSampler());

        $span = $this->createSpanWithParent(TraceFlags::sampled(), isRemote: true);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::DROP, $result->decision);
    }

    public function test_local_not_sampled_parent_drops_by_default(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler());

        $span = $this->createSpanWithParent(TraceFlags::default(), isRemote: false);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::DROP, $result->decision);
    }

    public function test_local_sampled_parent_samples_by_default(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOffSampler());

        $span = $this->createSpanWithParent(TraceFlags::sampled(), isRemote: false);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_remote_not_sampled_parent_drops_by_default(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler());

        $span = $this->createSpanWithParent(TraceFlags::default(), isRemote: true);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::DROP, $result->decision);
    }

    public function test_remote_sampled_parent_samples_by_default(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOffSampler());

        $span = $this->createSpanWithParent(TraceFlags::sampled(), isRemote: true);
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_root_span_uses_root_sampler_always_off(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOffSampler());

        $span = $this->createRootSpan();
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::DROP, $result->decision);
    }

    public function test_root_span_uses_root_sampler_always_on(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler());

        $span = $this->createRootSpan();
        $result = $sampler->shouldSample($span);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
    }

    public function test_to_string_includes_root_sampler(): void
    {
        $sampler = new ParentBasedSampler(new AlwaysOnSampler());

        static::assertStringContainsString('ParentBased', (string) $sampler);
        static::assertStringContainsString('AlwaysOnSampler', (string) $sampler);
    }

    private function createRootSpan(): Span
    {
        return new Span(
            'root-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }

    private function createSpanWithParent(TraceFlags $traceFlags, bool $isRemote): Span
    {
        $context = $isRemote
            ? SpanContext::createRemote(TraceId::generate(), SpanId::generate(), SpanId::generate(), $traceFlags)
            : SpanContext::create(TraceId::generate(), SpanId::generate(), SpanId::generate(), $traceFlags);

        return new Span(
            'child-span',
            $context,
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
