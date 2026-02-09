<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\{Baggage, Context, SpanId, TraceId};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ContextTest extends TestCase
{
    public static function provideBaggageEntries() : \Generator
    {
        yield 'empty' => [[]];
        yield 'single entry' => [['key' => 'value']];
        yield 'multiple entries' => [['user.id' => '12345', 'request.id' => 'abc-123']];
    }

    public static function provideContextConfigurations() : \Generator
    {
        yield 'root context' => [false];
        yield 'with active span' => [true];
    }

    public function test_active_span_id_returns_null_for_root_context() : void
    {
        $context = Context::create();

        self::assertNull($context->activeSpanId());
    }

    public function test_active_span_id_returns_span_id_when_set() : void
    {
        $spanId = SpanId::generate();
        $context = Context::create()->withActiveSpan($spanId);

        self::assertNotNull($context->activeSpanId());
        self::assertTrue($context->activeSpanId()->equals($spanId));
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_constructor_accepts_trace_id_and_baggage(array $entries) : void
    {
        $traceId = TraceId::generate();
        $baggage = new Baggage($entries);

        $context = new Context($traceId, $baggage);

        self::assertTrue($context->traceId->equals($traceId));
        self::assertSame($entries, $context->baggage->all());
    }

    public function test_create_returns_context_with_invalid_trace_id() : void
    {
        $context = Context::create();

        self::assertFalse($context->traceId->isValid());
        self::assertSame(TraceId::INVALID, $context->traceId->toHex());
        self::assertTrue($context->baggage->isEmpty());
        self::assertNull($context->activeSpanId());
    }

    public function test_default_baggage_is_empty() : void
    {
        $context = new Context(TraceId::generate());

        self::assertTrue($context->baggage->isEmpty());
    }

    public function test_from_array_creates_context_with_active_span() : void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => ['hex' => '00f067aa0ba902b7'],
        ];

        $context = Context::fromArray($data);

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        self::assertSame('12345', $context->baggage->get('user.id'));
        self::assertNotNull($context->activeSpanId());
        self::assertSame('00f067aa0ba902b7', $context->activeSpanId()->toHex());
    }

    public function test_from_array_creates_context_without_active_span() : void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => null,
        ];

        $context = Context::fromArray($data);

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        self::assertSame('12345', $context->baggage->get('user.id'));
        self::assertNull($context->activeSpanId());
    }

    #[DataProvider('provideContextConfigurations')]
    public function test_is_root_context_based_on_active_span(bool $hasActiveSpan) : void
    {
        $context = Context::create();

        if ($hasActiveSpan) {
            $context = $context->withActiveSpan(SpanId::generate());
        }

        self::assertSame(!$hasActiveSpan, $context->isRootContext());
    }

    public function test_normalize_from_array_round_trip_with_active_span() : void
    {
        $original = (new Context(
            TraceId::generate(),
            new Baggage(['a' => '1', 'b' => '2'])
        ))->withActiveSpan(SpanId::generate());

        $normalized = $original->normalize();
        $restored = Context::fromArray($normalized);

        self::assertTrue($original->traceId->equals($restored->traceId));
        self::assertSame($original->baggage->all(), $restored->baggage->all());
        self::assertNotNull($original->activeSpanId());
        self::assertNotNull($restored->activeSpanId());
        self::assertTrue($original->activeSpanId()->equals($restored->activeSpanId()));
    }

    public function test_normalize_from_array_round_trip_without_active_span() : void
    {
        $original = new Context(
            TraceId::generate(),
            new Baggage(['a' => '1', 'b' => '2'])
        );

        $normalized = $original->normalize();
        $restored = Context::fromArray($normalized);

        self::assertTrue($original->traceId->equals($restored->traceId));
        self::assertSame($original->baggage->all(), $restored->baggage->all());
        self::assertNull($restored->activeSpanId());
    }

    public function test_normalize_returns_array_with_active_span() : void
    {
        $context = (new Context(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            new Baggage(['user.id' => '12345'])
        ))->withActiveSpan(SpanId::fromHex('00f067aa0ba902b7'));

        $normalized = $context->normalize();

        self::assertSame([
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => ['hex' => '00f067aa0ba902b7'],
        ], $normalized);
    }

    public function test_normalize_returns_array_without_active_span() : void
    {
        $context = new Context(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            new Baggage(['user.id' => '12345'])
        );

        $normalized = $context->normalize();

        self::assertSame([
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => null,
        ], $normalized);
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_with_active_span_preserves_baggage(array $entries) : void
    {
        $baggage = new Baggage($entries);
        $context = new Context(TraceId::generate(), $baggage);

        $newContext = $context->withActiveSpan(SpanId::generate());

        self::assertSame($entries, $newContext->baggage->all());
    }

    public function test_with_active_span_preserves_trace_id() : void
    {
        $traceId = TraceId::generate();
        $context = new Context($traceId);

        $newContext = $context->withActiveSpan(SpanId::generate());

        self::assertTrue($newContext->traceId->equals($traceId));
    }

    public function test_with_active_span_sets_span_id() : void
    {
        $context = Context::create();
        $spanId = SpanId::generate();

        $newContext = $context->withActiveSpan($spanId);

        self::assertNull($context->activeSpanId());
        self::assertNotNull($newContext->activeSpanId());
        self::assertTrue($newContext->activeSpanId()->equals($spanId));
    }

    public function test_with_baggage_preserves_trace_id_and_active_span() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = Context::withTraceId($traceId)->withActiveSpan($spanId);

        $newContext = $context->withBaggage(new Baggage(['key' => 'value']));

        self::assertTrue($newContext->traceId->equals($traceId));
        self::assertNotNull($newContext->activeSpanId());
        self::assertTrue($newContext->activeSpanId()->equals($spanId));
    }

    public function test_with_baggage_replaces_baggage() : void
    {
        $context = new Context(TraceId::generate(), new Baggage(['old' => 'value']));
        $newBaggage = new Baggage(['new' => 'entry']);

        $newContext = $context->withBaggage($newBaggage);

        self::assertSame('value', $context->baggage->get('old'));
        self::assertNull($newContext->baggage->get('old'));
        self::assertSame('entry', $newContext->baggage->get('new'));
    }

    public function test_with_trace_id_creates_context_with_specific_trace_id() : void
    {
        $traceId = TraceId::generate();
        $context = Context::withTraceId($traceId);

        self::assertTrue($context->traceId->equals($traceId));
        self::assertTrue($context->baggage->isEmpty());
        self::assertNull($context->activeSpanId());
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_without_active_span_preserves_baggage(array $entries) : void
    {
        $baggage = new Baggage($entries);
        $context = (new Context(TraceId::generate(), $baggage))
            ->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        self::assertSame($entries, $newContext->baggage->all());
    }

    public function test_without_active_span_preserves_trace_id() : void
    {
        $traceId = TraceId::generate();
        $context = (new Context($traceId))
            ->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        self::assertTrue($newContext->traceId->equals($traceId));
    }

    public function test_without_active_span_removes_span_id() : void
    {
        $context = Context::create()
            ->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        self::assertNotNull($context->activeSpanId());
        self::assertNull($newContext->activeSpanId());
    }
}
