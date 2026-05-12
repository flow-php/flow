<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class SpanContextTest extends TestCase
{
    public function test_constructor_creates_span_context(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = new SpanContext($traceId, $spanId, $parentSpanId, true);

        static::assertTrue($context->traceId->equals($traceId));
        static::assertTrue($context->spanId->equals($spanId));
        static::assertNotNull($context->parentSpanId);
        static::assertTrue($context->parentSpanId->equals($parentSpanId));
        static::assertTrue($context->isRemote);
    }

    public function test_constructor_defaults_trace_flags_and_trace_state(): void
    {
        $context = new SpanContext(TraceId::generate(), SpanId::generate());

        static::assertFalse($context->traceFlags->isSampled());
        static::assertTrue($context->traceState->isEmpty());
    }

    public function test_constructor_with_defaults(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = new SpanContext($traceId, $spanId);

        static::assertNull($context->parentSpanId);
        static::assertFalse($context->isRemote);
    }

    public function test_constructor_with_trace_flags_and_trace_state(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('vendor', 'value');

        $context = new SpanContext($traceId, $spanId, null, false, $traceFlags, $traceState);

        static::assertTrue($context->traceFlags->isSampled());
        static::assertSame('value', $context->traceState->get('vendor'));
    }

    public function test_create_creates_local_span_context(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = SpanContext::create($traceId, $spanId);

        static::assertTrue($context->traceId->equals($traceId));
        static::assertTrue($context->spanId->equals($spanId));
        static::assertNull($context->parentSpanId);
        static::assertFalse($context->isRemote);
    }

    public function test_create_creates_local_span_context_with_parent(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = SpanContext::create($traceId, $spanId, $parentSpanId);

        static::assertNotNull($context->parentSpanId);
        static::assertTrue($context->parentSpanId->equals($parentSpanId));
        static::assertFalse($context->isRemote);
    }

    public function test_create_remote_creates_remote_span_context(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $context = SpanContext::createRemote($traceId, $spanId);

        static::assertTrue($context->traceId->equals($traceId));
        static::assertTrue($context->spanId->equals($spanId));
        static::assertNull($context->parentSpanId);
        static::assertTrue($context->isRemote);
    }

    public function test_create_remote_creates_remote_span_context_with_parent(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();

        $context = SpanContext::createRemote($traceId, $spanId, $parentSpanId);

        static::assertNotNull($context->parentSpanId);
        static::assertTrue($context->parentSpanId->equals($parentSpanId));
        static::assertTrue($context->isRemote);
    }

    public function test_create_remote_with_trace_flags_and_trace_state(): void
    {
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('key', 'val');

        $context = SpanContext::createRemote(TraceId::generate(), SpanId::generate(), null, $traceFlags, $traceState);

        static::assertTrue($context->isRemote);
        static::assertTrue($context->traceFlags->isSampled());
        static::assertSame('val', $context->traceState->get('key'));
    }

    public function test_create_with_trace_flags_and_trace_state(): void
    {
        $traceFlags = TraceFlags::sampled();
        $traceState = TraceState::empty()->with('key', 'val');

        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, $traceFlags, $traceState);

        static::assertTrue($context->traceFlags->isSampled());
        static::assertSame('val', $context->traceState->get('key'));
    }

    public function test_from_array_creates_span_context_with_parent(): void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'spanId' => ['hex' => '00f067aa0ba902b7'],
            'parentSpanId' => ['hex' => '11f067aa0ba902b8'],
            'isRemote' => true,
        ];

        $context = SpanContext::fromArray($data);

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $context->spanId->toHex());
        static::assertNotNull($context->parentSpanId);
        static::assertSame('11f067aa0ba902b8', $context->parentSpanId->toHex());
        static::assertTrue($context->isRemote);
    }

    public function test_from_array_creates_span_context_without_parent(): void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'spanId' => ['hex' => '00f067aa0ba902b7'],
            'parentSpanId' => null,
            'isRemote' => false,
        ];

        $context = SpanContext::fromArray($data);

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $context->spanId->toHex());
        static::assertNull($context->parentSpanId);
        static::assertFalse($context->isRemote);
    }

    public function test_from_array_restores_trace_flags_and_trace_state(): void
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

        static::assertTrue($context->traceFlags->isSampled());
        static::assertSame('data', $context->traceState->get('vendor'));
    }

    public function test_get_invalid_returns_invalid_context(): void
    {
        $context = SpanContext::getInvalid();

        static::assertFalse($context->traceId->isValid());
        static::assertFalse($context->spanId->isValid());
        static::assertFalse($context->isValid());
    }

    public function test_is_root_returns_false_when_parent_exists(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), SpanId::generate());

        static::assertFalse($context->isRoot());
    }

    public function test_is_root_returns_true_when_no_parent(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        static::assertTrue($context->isRoot());
    }

    public function test_is_valid_returns_false_for_invalid_context(): void
    {
        $context = SpanContext::getInvalid();

        static::assertFalse($context->isValid());
    }

    public function test_is_valid_returns_false_with_invalid_span_id(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::invalid());

        static::assertFalse($context->isValid());
    }

    public function test_is_valid_returns_false_with_invalid_trace_id(): void
    {
        $context = SpanContext::create(TraceId::invalid(), SpanId::generate());

        static::assertFalse($context->isValid());
    }

    public function test_is_valid_returns_true_for_valid_context(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        static::assertTrue($context->isValid());
    }

    public function test_normalize_from_array_round_trip_with_parent(): void
    {
        $original = SpanContext::create(TraceId::generate(), SpanId::generate(), SpanId::generate());

        $normalized = $original->normalize();
        $restored = SpanContext::fromArray($normalized);

        static::assertTrue($original->traceId->equals($restored->traceId));
        static::assertTrue($original->spanId->equals($restored->spanId));
        $originalParentSpanId = $original->parentSpanId;
        $restoredParentSpanId = $restored->parentSpanId;
        static::assertNotNull($originalParentSpanId);
        static::assertNotNull($restoredParentSpanId);
        static::assertTrue($originalParentSpanId->equals($restoredParentSpanId));
        static::assertSame($original->isRemote, $restored->isRemote);
    }

    public function test_normalize_from_array_round_trip_with_trace_flags_and_state(): void
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

        static::assertTrue($original->traceId->equals($restored->traceId));
        static::assertTrue($original->spanId->equals($restored->spanId));
        static::assertSame($original->traceFlags->toByte(), $restored->traceFlags->toByte());
        static::assertSame($original->traceState->get('rojo'), $restored->traceState->get('rojo'));
    }

    public function test_normalize_from_array_round_trip_without_parent(): void
    {
        $original = SpanContext::create(TraceId::generate(), SpanId::generate());

        $normalized = $original->normalize();
        $restored = SpanContext::fromArray($normalized);

        static::assertTrue($original->traceId->equals($restored->traceId));
        static::assertTrue($original->spanId->equals($restored->spanId));
        static::assertNull($restored->parentSpanId);
        static::assertSame($original->isRemote, $restored->isRemote);
    }

    public function test_normalize_includes_trace_flags_and_trace_state(): void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('key', 'value'),
        );

        $normalized = $context->normalize();

        static::assertArrayHasKey('traceFlags', $normalized);
        static::assertSame(1, $normalized['traceFlags']['byte']);
        static::assertArrayHasKey('traceState', $normalized);
        static::assertSame(['key' => 'value'], $normalized['traceState']['entries']);
    }

    public function test_normalize_returns_array_with_parent(): void
    {
        $context = new SpanContext(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            SpanId::fromHex('11f067aa0ba902b8'),
            true,
        );

        $normalized = $context->normalize();

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['traceId']['hex']);
        static::assertSame('00f067aa0ba902b7', $normalized['spanId']['hex']);
        static::assertIsArray($normalized['parentSpanId']);
        static::assertSame('11f067aa0ba902b8', $normalized['parentSpanId']['hex']);
        static::assertTrue($normalized['isRemote']);
        static::assertArrayHasKey('traceFlags', $normalized);
        static::assertArrayHasKey('traceState', $normalized);
    }

    public function test_normalize_returns_array_without_parent(): void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );

        $normalized = $context->normalize();

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['traceId']['hex']);
        static::assertSame('00f067aa0ba902b7', $normalized['spanId']['hex']);
        static::assertNull($normalized['parentSpanId']);
        static::assertFalse($normalized['isRemote']);
    }

    public function test_with_trace_flags_returns_new_instance(): void
    {
        $original = SpanContext::create(TraceId::generate(), SpanId::generate());
        $modified = $original->withTraceFlags(TraceFlags::sampled());

        static::assertNotSame($original, $modified);
        static::assertFalse($original->traceFlags->isSampled());
        static::assertTrue($modified->traceFlags->isSampled());
        static::assertTrue($original->traceId->equals($modified->traceId));
    }

    public function test_with_trace_state_returns_new_instance(): void
    {
        $original = SpanContext::create(TraceId::generate(), SpanId::generate());
        $newState = TraceState::empty()->with('vendor', 'data');
        $modified = $original->withTraceState($newState);

        static::assertNotSame($original, $modified);
        static::assertTrue($original->traceState->isEmpty());
        static::assertSame('data', $modified->traceState->get('vendor'));
    }
}
