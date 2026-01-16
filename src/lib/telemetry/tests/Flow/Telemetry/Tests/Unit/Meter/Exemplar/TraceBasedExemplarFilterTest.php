<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId};
use Flow\Telemetry\Meter\Exemplar\TraceBasedExemplarFilter;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class TraceBasedExemplarFilterTest extends TestCase
{
    public function test_returns_correct_string_representation() : void
    {
        $filter = new TraceBasedExemplarFilter();

        self::assertSame('TraceBasedExemplarFilter', (string) $filter);
    }

    public function test_should_sample_respects_sampled_flag_only() : void
    {
        $filter = new TraceBasedExemplarFilter();

        $sampledOnly = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()
        );

        $randomOnly = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::default()->withRandom(true)
        );

        $bothFlags = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()->withRandom(true)
        );

        self::assertTrue($filter->shouldSample($sampledOnly, 1, []));
        self::assertFalse($filter->shouldSample($randomOnly, 1, []));
        self::assertTrue($filter->shouldSample($bothFlags, 1, []));
    }

    public function test_should_sample_returns_false_when_context_is_not_sampled() : void
    {
        $filter = new TraceBasedExemplarFilter();
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::default()
        );

        self::assertFalse($filter->shouldSample($context, 100, []));
        self::assertFalse($filter->shouldSample($context, 42.5, ['http.method' => 'GET']));
    }

    public function test_should_sample_returns_false_when_context_is_null() : void
    {
        $filter = new TraceBasedExemplarFilter();

        self::assertFalse($filter->shouldSample(null, 100, []));
    }

    public function test_should_sample_returns_true_when_context_is_sampled() : void
    {
        $filter = new TraceBasedExemplarFilter();
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()
        );

        self::assertTrue($filter->shouldSample($context, 100, []));
    }

    public function test_should_sample_returns_true_when_context_is_sampled_with_attributes() : void
    {
        $filter = new TraceBasedExemplarFilter();
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()
        );

        self::assertTrue($filter->shouldSample($context, 42.5, ['http.method' => 'GET', 'http.status' => 200]));
    }
}
