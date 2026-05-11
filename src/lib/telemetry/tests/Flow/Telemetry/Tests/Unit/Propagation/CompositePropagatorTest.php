<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class CompositePropagatorTest extends TestCase
{
    public function test_extract_merges_contexts_from_all_propagators(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'baggage' => 'key=value',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        static::assertNotNull($ctx->baggage);
        static::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_extract_with_empty_propagators(): void
    {
        $propagator = new CompositePropagator([]);

        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNull($ctx->spanContext);
        static::assertNull($ctx->baggage);
    }

    public function test_extract_with_only_baggage_propagator(): void
    {
        $propagator = new CompositePropagator([
            new W3CBaggage(),
        ]);

        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'baggage' => 'key=value',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNull($ctx->spanContext);
        static::assertNotNull($ctx->baggage);
        static::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_extract_with_only_trace_context_propagator(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
        ]);

        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'baggage' => 'key=value',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        static::assertNull($ctx->baggage);
    }

    public function test_fields_merges_all_fields(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $fields = $propagator->fields();

        static::assertContains('traceparent', $fields);
        static::assertContains('tracestate', $fields);
        static::assertContains('baggage', $fields);
    }

    public function test_fields_returns_empty_for_no_propagators(): void
    {
        $propagator = new CompositePropagator([]);

        static::assertSame([], $propagator->fields());
    }

    public function test_fields_returns_unique_values(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CTraceContext(),
        ]);

        $fields = $propagator->fields();

        static::assertCount(2, $fields);
        static::assertContains('traceparent', $fields);
        static::assertContains('tracestate', $fields);
    }

    public function test_inject_calls_all_propagators(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $baggage = new Baggage(['key' => 'value']);
        $ctx = new PropagationContext($spanContext, $baggage);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        static::assertArrayHasKey('traceparent', $headers);
        static::assertArrayHasKey('baggage', $headers);
        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $headers['traceparent']);
        static::assertSame('key=value', $headers['baggage']);
    }

    public function test_inject_does_nothing_for_empty_propagators(): void
    {
        $propagator = new CompositePropagator([]);

        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $ctx = new PropagationContext($spanContext);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertEmpty($carrier->unwrap());
    }

    public function test_inject_with_only_baggage(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $baggage = new Baggage(['key' => 'value']);
        $ctx = new PropagationContext(baggage: $baggage);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        static::assertArrayNotHasKey('traceparent', $headers);
        static::assertArrayHasKey('baggage', $headers);
    }

    public function test_inject_with_only_span_context(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        static::assertArrayHasKey('traceparent', $headers);
        static::assertArrayNotHasKey('baggage', $headers);
    }

    public function test_round_trip_preserves_full_context(): void
    {
        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $originalSpanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $originalBaggage = new Baggage(['user.id' => '12345', 'session.id' => 'abc-def']);
        $original = new PropagationContext($originalSpanContext, $originalBaggage);

        $carrier = new ArrayCarrier();
        $propagator->inject($original, $carrier);

        $restored = $propagator->extract(new ArrayCarrier($carrier->unwrap()));

        static::assertNotNull($restored->spanContext);
        static::assertNotNull($restored->baggage);
        static::assertSame($originalSpanContext->traceId->toHex(), $restored->spanContext->traceId->toHex());
        static::assertSame($originalSpanContext->spanId->toHex(), $restored->spanContext->spanId->toHex());
        static::assertSame($originalBaggage->get('user.id'), $restored->baggage->get('user.id'));
        static::assertSame($originalBaggage->get('session.id'), $restored->baggage->get('session.id'));
    }
}
