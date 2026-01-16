<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId, TraceState};
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class SpanContextTest extends TestCase
{
    public function test_constructor_creates_span_context() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = new SpanContext($traceId, $spanId, $parentSpanId, true);

        self::assertTrue($context->traceId->equals($traceId));
        self::assertTrue($context->spanId->equals($spanId));
        self::assertNotNull($context->parentSpanId);
        self::assertTrue($context->parentSpanId->equals($parentSpanId));
        self::assertTrue($context->isRemote);
    }

    public function test_constructor_defaults_trace_flags_and_trace_state() : void
    {
        $context = new SpanContext(
            TraceId::generate(),
            SpanId::generate(),
        );

        self::assertFalse($context->traceFlags->isSampled());
        self::assertTrue($context->traceState->isEmpty());
    }

    public function test_constructor_with_defaults() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = new SpanContext($traceId, $spanId);

        self::assertNull($context->parentSpanId);
        self::assertFalse($context->isRemote);
    }

    public function test_constructor_with_trace_flags_and_trace_state() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('vendor', 'value');

        $context = new SpanContext(
            $traceId,
            $spanId,
            null,
            false,
            $traceFlags,
            $traceState,
        );

        self::assertTrue($context->traceFlags->isSampled());
        self::assertSame('value', $context->traceState->get('vendor'));
    }

    public function test_create_creates_local_span_context() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = SpanContext::create($traceId, $spanId);

        self::assertTrue($context->traceId->equals($traceId));
        self::assertTrue($context->spanId->equals($spanId));
        self::assertNull($context->parentSpanId);
        self::assertFalse($context->isRemote);
    }

    public function test_create_creates_local_span_context_with_parent() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = SpanContext::create($traceId, $spanId, $parentSpanId);

        self::assertNotNull($context->parentSpanId);
        self::assertTrue($context->parentSpanId->equals($parentSpanId));
        self::assertFalse($context->isRemote);
    }

    public function test_create_remote_creates_remote_span_context() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = SpanContext::createRemote($traceId, $spanId);

        self::assertTrue($context->traceId->equals($traceId));
        self::assertTrue($context->spanId->equals($spanId));
        self::assertNull($context->parentSpanId);
        self::assertTrue($context->isRemote);
    }

    public function test_create_remote_creates_remote_span_context_with_parent() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = SpanContext::createRemote($traceId, $spanId, $parentSpanId);

        self::assertNotNull($context->parentSpanId);
        self::assertTrue($context->parentSpanId->equals($parentSpanId));
        self::assertTrue($context->isRemote);
    }

    public function test_create_remote_with_trace_flags_and_trace_state() : void
    {
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('key', 'val');

        $context = SpanContext::createRemote(
            TraceId::generate(),
            SpanId::generate(),
            null,
            $traceFlags,
            $traceState,
        );

        self::assertTrue($context->isRemote);
        self::assertTrue($context->traceFlags->isSampled());
        self::assertSame('val', $context->traceState->get('key'));
    }

    public function test_create_with_trace_flags_and_trace_state() : void
    {
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('key', 'val');

        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            $traceFlags,
            $traceState,
        );

        self::assertTrue($context->traceFlags->isSampled());
        self::assertSame('val', $context->traceState->get('key'));
    }

    public function test_from_array_creates_span_context_with_parent() : void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'spanId' => ['hex' => '00f067aa0ba902b7'],
            'parentSpanId' => ['hex' => '11f067aa0ba902b8'],
            'isRemote' => true,
        ];

        $context = SpanContext::fromArray($data);

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        self::assertSame('00f067aa0ba902b7', $context->spanId->toHex());
        self::assertNotNull($context->parentSpanId);
        self::assertSame('11f067aa0ba902b8', $context->parentSpanId->toHex());
        self::assertTrue($context->isRemote);
    }

    public function test_from_array_creates_span_context_without_parent() : void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'spanId' => ['hex' => '00f067aa0ba902b7'],
            'parentSpanId' => null,
            'isRemote' => false,
        ];

        $context = SpanContext::fromArray($data);

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        self::assertSame('00f067aa0ba902b7', $context->spanId->toHex());
        self::assertNull($context->parentSpanId);
        self::assertFalse($context->isRemote);
    }

    public function test_from_array_restores_trace_flags_and_trace_state() : void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'spanId' => ['hex' => '00f067aa0ba902b7'],
            'parentSpanId' => null,
            'isRemote' => true,
            'traceFlags' => ['byte' => 1],
            'traceState' => ['entries' => ['vendor' => 'data']],
        ];

        $context = SpanContext::fromArray($data);

        self::assertTrue($context->traceFlags->isSampled());
        self::assertSame('data', $context->traceState->get('vendor'));
    }

    public function test_is_root_returns_false_when_parent_exists() : void
    {
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            SpanId::generate(),
        );

        self::assertFalse($context->isRoot());
    }

    public function test_is_root_returns_true_when_no_parent() : void
    {
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );

        self::assertTrue($context->isRoot());
    }

    public function test_is_valid_returns_true_for_valid_context() : void
    {
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );

        self::assertTrue($context->isValid());
    }

    public function test_normalize_from_array_round_trip_with_parent() : void
    {
        $original = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            SpanId::generate(),
        );

        $normalized = $original->normalize();
        $restored = SpanContext::fromArray($normalized);

        self::assertTrue($original->traceId->equals($restored->traceId));
        self::assertTrue($original->spanId->equals($restored->spanId));
        self::assertNotNull($original->parentSpanId);
        self::assertNotNull($restored->parentSpanId);
        self::assertTrue($original->parentSpanId->equals($restored->parentSpanId));
        self::assertSame($original->isRemote, $restored->isRemote);
    }

    public function test_normalize_from_array_round_trip_with_trace_flags_and_state() : void
    {
        $original = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('rojo', '123'),
        );

        $normalized = $original->normalize();
        $restored = SpanContext::fromArray($normalized);

        self::assertTrue($original->traceId->equals($restored->traceId));
        self::assertTrue($original->spanId->equals($restored->spanId));
        self::assertSame($original->traceFlags->toByte(), $restored->traceFlags->toByte());
        self::assertSame($original->traceState->get('rojo'), $restored->traceState->get('rojo'));
    }

    public function test_normalize_from_array_round_trip_without_parent() : void
    {
        $original = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );

        $normalized = $original->normalize();
        $restored = SpanContext::fromArray($normalized);

        self::assertTrue($original->traceId->equals($restored->traceId));
        self::assertTrue($original->spanId->equals($restored->spanId));
        self::assertNull($restored->parentSpanId);
        self::assertSame($original->isRemote, $restored->isRemote);
    }

    public function test_normalize_includes_trace_flags_and_trace_state() : void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('key', 'value'),
        );

        $normalized = $context->normalize();

        self::assertArrayHasKey('traceFlags', $normalized);
        self::assertSame(1, $normalized['traceFlags']['byte']);
        self::assertArrayHasKey('traceState', $normalized);
        self::assertSame(['key' => 'value'], $normalized['traceState']['entries']);
    }

    public function test_normalize_returns_array_with_parent() : void
    {
        $context = new SpanContext(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            SpanId::fromHex('11f067aa0ba902b8'),
            true,
        );

        $normalized = $context->normalize();

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['traceId']['hex']);
        self::assertSame('00f067aa0ba902b7', $normalized['spanId']['hex']);
        self::assertIsArray($normalized['parentSpanId']);
        self::assertSame('11f067aa0ba902b8', $normalized['parentSpanId']['hex']);
        self::assertTrue($normalized['isRemote']);
        self::assertArrayHasKey('traceFlags', $normalized);
        self::assertArrayHasKey('traceState', $normalized);
    }

    public function test_normalize_returns_array_without_parent() : void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );

        $normalized = $context->normalize();

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['traceId']['hex']);
        self::assertSame('00f067aa0ba902b7', $normalized['spanId']['hex']);
        self::assertNull($normalized['parentSpanId']);
        self::assertFalse($normalized['isRemote']);
    }

    public function test_with_trace_flags_returns_new_instance() : void
    {
        $original = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );
        $modified = $original->withTraceFlags(TraceFlags::sampled());

        self::assertNotSame($original, $modified);
        self::assertFalse($original->traceFlags->isSampled());
        self::assertTrue($modified->traceFlags->isSampled());
        self::assertTrue($original->traceId->equals($modified->traceId));
    }

    public function test_with_trace_state_returns_new_instance() : void
    {
        $original = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );
        $newState = TraceState::empty()->with('vendor', 'data');
        $modified = $original->withTraceState($newState);

        self::assertNotSame($original, $modified);
        self::assertTrue($original->traceState->isEmpty());
        self::assertSame('data', $modified->traceState->get('vendor'));
    }
}
