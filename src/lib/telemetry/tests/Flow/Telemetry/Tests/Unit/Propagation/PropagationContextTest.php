<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class PropagationContextTest extends TestCase
{
    public function test_constructor_with_both_values(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $baggage = new Baggage(['key' => 'value']);

        $ctx = new PropagationContext($spanContext, $baggage);

        static::assertSame($spanContext, $ctx->spanContext);
        static::assertSame($baggage, $ctx->baggage);
    }

    public function test_constructor_with_defaults(): void
    {
        $ctx = new PropagationContext();

        static::assertNull($ctx->spanContext);
        static::assertNull($ctx->baggage);
    }

    public function test_constructor_with_only_baggage(): void
    {
        $baggage = new Baggage(['key' => 'value']);

        $ctx = new PropagationContext(baggage: $baggage);

        static::assertNull($ctx->spanContext);
        static::assertSame($baggage, $ctx->baggage);
    }

    public function test_constructor_with_only_span_context(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );

        $ctx = new PropagationContext($spanContext);

        static::assertSame($spanContext, $ctx->spanContext);
        static::assertNull($ctx->baggage);
    }

    public function test_merge_overwrites_null_baggage(): void
    {
        $baggage = new Baggage(['key' => 'value']);
        $ctx1 = new PropagationContext();
        $ctx2 = new PropagationContext(baggage: $baggage);

        $merged = $ctx1->merge($ctx2);

        static::assertSame($baggage, $merged->baggage);
    }

    public function test_merge_overwrites_null_span_context(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $ctx1 = new PropagationContext();
        $ctx2 = new PropagationContext($spanContext);

        $merged = $ctx1->merge($ctx2);

        static::assertSame($spanContext, $merged->spanContext);
    }

    public function test_merge_preserves_existing_when_other_is_null(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $baggage = new Baggage(['key' => 'value']);
        $ctx1 = new PropagationContext($spanContext, $baggage);
        $ctx2 = new PropagationContext();

        $merged = $ctx1->merge($ctx2);

        static::assertSame($spanContext, $merged->spanContext);
        static::assertSame($baggage, $merged->baggage);
    }

    public function test_merge_replaces_existing_with_other_values(): void
    {
        $spanContext1 = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $spanContext2 = SpanContext::create(
            TraceId::fromHex('1af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('10f067aa0ba902b7'),
        );
        $baggage1 = new Baggage(['key1' => 'value1']);
        $baggage2 = new Baggage(['key2' => 'value2']);

        $ctx1 = new PropagationContext($spanContext1, $baggage1);
        $ctx2 = new PropagationContext($spanContext2, $baggage2);

        $merged = $ctx1->merge($ctx2);

        static::assertSame($spanContext2, $merged->spanContext);
        static::assertSame($baggage2, $merged->baggage);
    }

    public function test_with_baggage_creates_new_instance(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $baggage1 = new Baggage(['key1' => 'value1']);
        $baggage2 = new Baggage(['key2' => 'value2']);

        $ctx = new PropagationContext($spanContext, $baggage1);
        $newCtx = $ctx->withBaggage($baggage2);

        static::assertNotSame($ctx, $newCtx);
        static::assertSame($baggage1, $ctx->baggage);
        static::assertSame($baggage2, $newCtx->baggage);
        static::assertSame($spanContext, $newCtx->spanContext);
    }

    public function test_with_baggage_null(): void
    {
        $baggage = new Baggage(['key' => 'value']);
        $ctx = new PropagationContext(baggage: $baggage);

        $newCtx = $ctx->withBaggage(null);

        static::assertNull($newCtx->baggage);
    }

    public function test_with_span_context_creates_new_instance(): void
    {
        $spanContext1 = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $spanContext2 = SpanContext::create(
            TraceId::fromHex('1af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('10f067aa0ba902b7'),
        );
        $baggage = new Baggage(['key' => 'value']);

        $ctx = new PropagationContext($spanContext1, $baggage);
        $newCtx = $ctx->withSpanContext($spanContext2);

        static::assertNotSame($ctx, $newCtx);
        static::assertSame($spanContext1, $ctx->spanContext);
        static::assertSame($spanContext2, $newCtx->spanContext);
        static::assertSame($baggage, $newCtx->baggage);
    }

    public function test_with_span_context_null(): void
    {
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $ctx = new PropagationContext($spanContext);

        $newCtx = $ctx->withSpanContext(null);

        static::assertNull($newCtx->spanContext);
    }
}
