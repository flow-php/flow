<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Sampler\SamplingResult;
use PHPUnit\Framework\TestCase;

final class SamplingResultTest extends TestCase
{
    public function test_constructor_with_all_parameters(): void
    {
        $traceState = TraceState::empty()->with('vendor', 'data');
        $attributes = ['sampling.source' => 'test'];

        $result = new SamplingResult(SamplingDecision::RECORD_AND_SAMPLE, $attributes, $traceState);

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
        static::assertSame($attributes, $result->attributes);
        static::assertSame($traceState, $result->traceState);
    }

    public function test_drop_factory(): void
    {
        $result = SamplingResult::drop();

        static::assertSame(SamplingDecision::DROP, $result->decision);
        static::assertSame([], $result->attributes);
        static::assertNull($result->traceState);
    }

    public function test_record_and_sample_factory(): void
    {
        $result = SamplingResult::recordAndSample();

        static::assertSame(SamplingDecision::RECORD_AND_SAMPLE, $result->decision);
        static::assertSame([], $result->attributes);
        static::assertNull($result->traceState);
    }

    public function test_record_and_sample_with_attributes(): void
    {
        $attributes = ['sampling.reason' => 'head'];
        $result = SamplingResult::recordAndSample($attributes);

        static::assertSame($attributes, $result->attributes);
    }

    public function test_record_and_sample_with_trace_state(): void
    {
        $traceState = TraceState::empty()->with('key', 'value');
        $result = SamplingResult::recordAndSample([], $traceState);

        static::assertSame($traceState, $result->traceState);
    }

    public function test_record_only_factory(): void
    {
        $result = SamplingResult::recordOnly();

        static::assertSame(SamplingDecision::RECORD_ONLY, $result->decision);
        static::assertSame([], $result->attributes);
        static::assertNull($result->traceState);
    }

    public function test_record_only_with_attributes(): void
    {
        $attributes = ['sampling.reason' => 'custom'];
        $result = SamplingResult::recordOnly($attributes);

        static::assertSame($attributes, $result->attributes);
    }

    public function test_record_only_with_trace_state(): void
    {
        $traceState = TraceState::empty()->with('vendor', 'data');
        $result = SamplingResult::recordOnly([], $traceState);

        static::assertSame($traceState, $result->traceState);
    }
}
