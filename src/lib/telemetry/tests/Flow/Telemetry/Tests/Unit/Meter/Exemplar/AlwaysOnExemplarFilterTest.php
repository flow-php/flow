<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Meter\Exemplar\AlwaysOnExemplarFilter;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class AlwaysOnExemplarFilterTest extends TestCase
{
    public function test_returns_correct_string_representation(): void
    {
        $filter = new AlwaysOnExemplarFilter();

        static::assertSame('AlwaysOnExemplarFilter', (string) $filter);
    }

    public function test_should_sample_returns_false_when_context_is_null(): void
    {
        $filter = new AlwaysOnExemplarFilter();

        static::assertFalse($filter->shouldSample(null, 100, []));
    }

    public function test_should_sample_returns_true_regardless_of_sampled_flag(): void
    {
        $filter = new AlwaysOnExemplarFilter();

        $sampledContext = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());

        $unsampledContext = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::default());

        static::assertTrue($filter->shouldSample($sampledContext, 100, []));
        static::assertTrue($filter->shouldSample($unsampledContext, 100, []));
    }

    public function test_should_sample_returns_true_when_context_exists(): void
    {
        $filter = new AlwaysOnExemplarFilter();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        static::assertTrue($filter->shouldSample($context, 100, []));
    }

    public function test_should_sample_returns_true_when_context_exists_with_attributes(): void
    {
        $filter = new AlwaysOnExemplarFilter();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        static::assertTrue($filter->shouldSample($context, 42.5, ['http.method' => 'GET']));
    }
}
