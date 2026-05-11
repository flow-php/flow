<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\SpanContext;

final class SpanContextMother
{
    public static function create(
        ?TraceId $traceId = null,
        ?SpanId $spanId = null,
        ?SpanId $parentSpanId = null,
    ): SpanContext {
        return SpanContext::create(
            $traceId ?? TraceId::generate(),
            $spanId ?? SpanId::generate(),
            $parentSpanId,
            TraceFlags::sampled(),
        );
    }

    public static function withFixedIds(): SpanContext
    {
        return SpanContext::create(
            TraceId::fromHex('0102030405060708090a0b0c0d0e0f10'),
            SpanId::fromHex('0102030405060708'),
            null,
            TraceFlags::sampled(),
        );
    }
}
