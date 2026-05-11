<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class W3CTraceContextTest extends TestCase
{
    public static function provideInvalidTraceparentHeaders(): \Generator
    {
        yield 'wrong version' => ['01-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01'];
        yield 'missing parts' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7'];
        yield 'too many parts' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01-extra'];
        yield 'short trace id' => ['00-0af7651916cd43dd-00f067aa0ba902b7-01'];
        yield 'long trace id' => ['00-0af7651916cd43dd8448eb211c80319c00-00f067aa0ba902b7-01'];
        yield 'invalid hex in trace id' => ['00-0af7651916cd43dd8448eb211c80319g-00f067aa0ba902b7-01'];
        yield 'short span id' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa-01'];
        yield 'long span id' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b700-01'];
        yield 'invalid hex in span id' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902bg-01'];
        yield 'short flags' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-1'];
        yield 'long flags' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-011'];
        yield 'invalid hex in flags' => ['00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-0g'];
        yield 'all zeros trace id' => ['00-00000000000000000000000000000000-00f067aa0ba902b7-01'];
        yield 'all zeros span id' => ['00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01'];
    }

    public static function provideValidTraceparentHeaders(): \Generator
    {
        yield 'sampled' => [
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            '0af7651916cd43dd8448eb211c80319c',
            '00f067aa0ba902b7',
            true,
        ];
        yield 'not sampled' => [
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-00',
            '0af7651916cd43dd8448eb211c80319c',
            '00f067aa0ba902b7',
            false,
        ];
        yield 'uppercase' => [
            '00-0AF7651916CD43DD8448EB211C80319C-00F067AA0BA902B7-01',
            '0af7651916cd43dd8448eb211c80319c',
            '00f067aa0ba902b7',
            true,
        ];
    }

    public function test_extract_handles_invalid_tracestate_gracefully(): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'invalid tracestate !@#$',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertTrue($ctx->spanContext->traceState->isEmpty());
    }

    public function test_extract_parses_tracestate_header(): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('00f067aa0ba902b7', $ctx->spanContext->traceState->get('rojo'));
        static::assertSame('t61rcWkgMzE', $ctx->spanContext->traceState->get('congo'));
    }

    #[DataProvider('provideValidTraceparentHeaders')]
    public function test_extract_parses_valid_traceparent(
        string $traceparent,
        string $expectedTraceId,
        string $expectedSpanId,
        bool $expectedSampled,
    ): void {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier(['traceparent' => $traceparent]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame($expectedTraceId, $ctx->spanContext->traceId->toHex());
        static::assertSame($expectedSpanId, $ctx->spanContext->spanId->toHex());
        static::assertSame($expectedSampled, $ctx->spanContext->traceFlags->isSampled());
        static::assertTrue($ctx->spanContext->isRemote);
    }

    #[DataProvider('provideInvalidTraceparentHeaders')]
    public function test_extract_returns_empty_context_for_invalid_traceparent(string $traceparent): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier(['traceparent' => $traceparent]);

        $ctx = $propagator->extract($carrier);

        static::assertNull($ctx->spanContext);
    }

    public function test_extract_returns_empty_context_for_missing_header(): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier([]);

        $ctx = $propagator->extract($carrier);

        static::assertNull($ctx->spanContext);
    }

    public function test_extract_returns_propagation_context_without_baggage(): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier([
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertNull($ctx->baggage);
    }

    public function test_extract_with_case_insensitive_headers(): void
    {
        $propagator = new W3CTraceContext();
        $carrier = new ArrayCarrier([
            'Traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'Tracestate' => 'rojo=value',
        ]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        static::assertSame('value', $ctx->spanContext->traceState->get('rojo'));
    }

    public function test_fields_returns_correct_header_names(): void
    {
        $propagator = new W3CTraceContext();

        static::assertSame(['traceparent', 'tracestate'], $propagator->fields());
    }

    public function test_inject_does_not_set_tracestate_when_empty(): void
    {
        $propagator = new W3CTraceContext();
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $ctx = new PropagationContext($spanContext);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        static::assertArrayHasKey('traceparent', $headers);
        static::assertArrayNotHasKey('tracestate', $headers);
    }

    public function test_inject_does_nothing_for_null_span_context(): void
    {
        $propagator = new W3CTraceContext();
        $ctx = new PropagationContext();
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertEmpty($carrier->unwrap());
    }

    public function test_inject_sets_traceparent_header(): void
    {
        $propagator = new W3CTraceContext();
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $carrier->unwrap()['traceparent'],
        );
    }

    public function test_inject_sets_tracestate_header_when_not_empty(): void
    {
        $propagator = new W3CTraceContext();
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('rojo', 'value'),
        );
        $ctx = new PropagationContext($spanContext);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertSame('rojo=value', $carrier->unwrap()['tracestate']);
    }

    public function test_round_trip_preserves_context(): void
    {
        $propagator = new W3CTraceContext();
        $original = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('vendor', 'data'),
        );
        $ctx = new PropagationContext($original);
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);
        $restored = $propagator->extract(new ArrayCarrier($carrier->unwrap()));

        static::assertNotNull($restored->spanContext);
        static::assertSame($original->traceId->toHex(), $restored->spanContext->traceId->toHex());
        static::assertSame($original->spanId->toHex(), $restored->spanContext->spanId->toHex());
        static::assertSame($original->traceFlags->isSampled(), $restored->spanContext->traceFlags->isSampled());
        static::assertSame($original->traceState->get('vendor'), $restored->spanContext->traceState->get('vendor'));
    }
}
