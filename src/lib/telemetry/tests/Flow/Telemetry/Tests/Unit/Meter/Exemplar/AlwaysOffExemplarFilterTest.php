<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Exemplar;

use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId};
use Flow\Telemetry\Meter\Exemplar\AlwaysOffExemplarFilter;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class AlwaysOffExemplarFilterTest extends TestCase
{
    public function test_returns_correct_string_representation() : void
    {
        $filter = new AlwaysOffExemplarFilter();

        self::assertSame('AlwaysOffExemplarFilter', (string) $filter);
    }

    public function test_should_sample_always_returns_false() : void
    {
        $filter = new AlwaysOffExemplarFilter();

        self::assertFalse($filter->shouldSample(null, 0, []));
        self::assertFalse($filter->shouldSample(null, 100, ['key' => 'value']));
        self::assertFalse($filter->shouldSample(
            SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled()),
            999.9,
            ['attr1' => 'val1', 'attr2' => 123]
        ));
    }

    public function test_should_sample_returns_false_when_context_exists() : void
    {
        $filter = new AlwaysOffExemplarFilter();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        self::assertFalse($filter->shouldSample($context, 100, []));
    }

    public function test_should_sample_returns_false_when_context_is_null() : void
    {
        $filter = new AlwaysOffExemplarFilter();

        self::assertFalse($filter->shouldSample(null, 100, []));
    }

    public function test_should_sample_returns_false_when_context_is_sampled() : void
    {
        $filter = new AlwaysOffExemplarFilter();
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()
        );

        self::assertFalse($filter->shouldSample($context, 42.5, ['http.method' => 'GET']));
    }
}
