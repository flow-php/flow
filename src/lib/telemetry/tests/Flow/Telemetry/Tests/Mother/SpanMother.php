<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind};

final class SpanMother
{
    public static function create(
        string $name = 'test-span',
        ?TraceId $traceId = null,
        ?SpanId $spanId = null,
        ?SpanId $parentSpanId = null,
        SpanKind $kind = SpanKind::INTERNAL,
        ?\DateTimeImmutable $startTime = null,
    ) : Span {
        return new Span(
            $name,
            SpanContext::create(
                $traceId ?? TraceId::generate(),
                $spanId ?? SpanId::generate(),
                $parentSpanId,
            ),
            $kind,
            $startTime ?? new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }

    public static function withName(string $name) : Span
    {
        return self::create($name);
    }

    public static function withTraceId(TraceId $traceId) : Span
    {
        return self::create('test-span', $traceId);
    }
}
