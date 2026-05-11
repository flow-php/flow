<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Meter\Exemplar\AlwaysOffExemplarFilter;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class AlwaysOffExemplarFilterTest extends TestCase
{
    public function test_returns_correct_string_representation(): void
    {
        $filter = new AlwaysOffExemplarFilter();

        static::assertSame('AlwaysOffExemplarFilter', (string) $filter);
    }

    public function test_should_sample_always_returns_false(): void
    {
        $filter = new AlwaysOffExemplarFilter();

        static::assertFalse($filter->shouldSample(null, 0, []));
        static::assertFalse($filter->shouldSample(null, 100, ['key' => 'value']));
        static::assertFalse($filter->shouldSample(
            SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled()),
            999.9,
            ['attr1' => 'val1', 'attr2' => 123],
        ));
    }

    public function test_should_sample_returns_false_when_context_exists(): void
    {
        $filter = new AlwaysOffExemplarFilter();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        static::assertFalse($filter->shouldSample($context, 100, []));
    }

    public function test_should_sample_returns_false_when_context_is_null(): void
    {
        $filter = new AlwaysOffExemplarFilter();

        static::assertFalse($filter->shouldSample(null, 100, []));
    }

    public function test_should_sample_returns_false_when_context_is_sampled(): void
    {
        $filter = new AlwaysOffExemplarFilter();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());

        static::assertFalse($filter->shouldSample($context, 42.5, ['http.method' => 'GET']));
    }
}
