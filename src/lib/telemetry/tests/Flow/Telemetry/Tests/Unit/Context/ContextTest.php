<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ContextTest extends TestCase
{
    public static function provideBaggageEntries(): Generator
    {
        yield 'empty' => [[]];
        yield 'single entry' => [['key' => 'value']];
        yield 'multiple entries' => [['user.id' => '12345', 'request.id' => 'abc-123']];
    }

    public static function provideContextConfigurations(): Generator
    {
        yield 'root context' => [false];
        yield 'with active span' => [true];
    }

    public function test_active_span_id_returns_null_for_root_context(): void
    {
        $context = Context::create();

        static::assertNull($context->activeSpanId());
    }

    public function test_active_span_id_returns_span_id_when_set(): void
    {
        $spanId = SpanId::generate();
        $context = Context::create()->withActiveSpan($spanId);

        $activeSpanId = $context->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertTrue($activeSpanId->equals($spanId));
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_constructor_accepts_trace_id_and_baggage(array $entries): void
    {
        $traceId = TraceId::generate();
        $baggage = new Baggage($entries);

        $context = new Context($traceId, $baggage);

        static::assertTrue($context->traceId->equals($traceId));
        static::assertSame($entries, $context->baggage->all());
    }

    public function test_create_returns_context_with_invalid_trace_id(): void
    {
        $context = Context::create();

        static::assertFalse($context->traceId->isValid());
        static::assertSame(TraceId::INVALID, $context->traceId->toHex());
        static::assertTrue($context->baggage->isEmpty());
        static::assertNull($context->activeSpanId());
    }

    public function test_default_baggage_is_empty(): void
    {
        $context = new Context(TraceId::generate());

        static::assertTrue($context->baggage->isEmpty());
    }

    public function test_from_array_creates_context_with_active_span(): void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => ['hex' => '00f067aa0ba902b7'],
        ];

        $context = Context::fromArray($data);

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        static::assertSame('12345', $context->baggage->get('user.id'));
        $activeSpanId = $context->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertSame('00f067aa0ba902b7', $activeSpanId->toHex());
    }

    public function test_from_array_creates_context_without_active_span(): void
    {
        $data = [
            'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
            'baggage' => ['entries' => ['user.id' => '12345']],
            'activeSpanId' => null,
        ];

        $context = Context::fromArray($data);

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $context->traceId->toHex());
        static::assertSame('12345', $context->baggage->get('user.id'));
        static::assertNull($context->activeSpanId());
    }

    #[DataProvider('provideContextConfigurations')]
    public function test_is_root_context_based_on_active_span(bool $hasActiveSpan): void
    {
        $context = Context::create();

        if ($hasActiveSpan) {
            $context = $context->withActiveSpan(SpanId::generate());
        }

        static::assertSame(!$hasActiveSpan, $context->isRootContext());
    }

    public function test_normalize_from_array_round_trip_with_active_span(): void
    {
        $original = (new Context(TraceId::generate(), new Baggage(['a' => '1', 'b' => '2'])))->withActiveSpan(
            SpanId::generate(),
        );

        $normalized = $original->normalize();
        $restored = Context::fromArray($normalized);

        static::assertTrue($original->traceId->equals($restored->traceId));
        static::assertSame($original->baggage->all(), $restored->baggage->all());
        $originalActiveSpanId = $original->activeSpanId();
        $restoredActiveSpanId = $restored->activeSpanId();
        static::assertNotNull($originalActiveSpanId);
        static::assertNotNull($restoredActiveSpanId);
        static::assertTrue($originalActiveSpanId->equals($restoredActiveSpanId));
    }

    public function test_normalize_from_array_round_trip_without_active_span(): void
    {
        $original = new Context(TraceId::generate(), new Baggage(['a' => '1', 'b' => '2']));

        $normalized = $original->normalize();
        $restored = Context::fromArray($normalized);

        static::assertTrue($original->traceId->equals($restored->traceId));
        static::assertSame($original->baggage->all(), $restored->baggage->all());
        static::assertNull($restored->activeSpanId());
    }

    public function test_normalize_returns_array_with_active_span(): void
    {
        $context = (new Context(TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'), new Baggage([
            'user.id' => '12345',
        ])))->withActiveSpan(SpanId::fromHex('00f067aa0ba902b7'));

        $normalized = $context->normalize();

        static::assertSame(
            [
                'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
                'baggage' => ['entries' => ['user.id' => '12345']],
                'activeSpanId' => ['hex' => '00f067aa0ba902b7'],
            ],
            $normalized,
        );
    }

    public function test_normalize_returns_array_without_active_span(): void
    {
        $context = new Context(TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'), new Baggage([
            'user.id' => '12345',
        ]));

        $normalized = $context->normalize();

        static::assertSame(
            [
                'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
                'baggage' => ['entries' => ['user.id' => '12345']],
                'activeSpanId' => null,
            ],
            $normalized,
        );
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_with_active_span_preserves_baggage(array $entries): void
    {
        $baggage = new Baggage($entries);
        $context = new Context(TraceId::generate(), $baggage);

        $newContext = $context->withActiveSpan(SpanId::generate());

        static::assertSame($entries, $newContext->baggage->all());
    }

    public function test_with_active_span_preserves_trace_id(): void
    {
        $traceId = TraceId::generate();
        $context = new Context($traceId);

        $newContext = $context->withActiveSpan(SpanId::generate());

        static::assertTrue($newContext->traceId->equals($traceId));
    }

    public function test_with_active_span_sets_span_id(): void
    {
        $context = Context::create();
        $spanId = SpanId::generate();

        $newContext = $context->withActiveSpan($spanId);

        static::assertNull($context->activeSpanId());
        $newActiveSpanId = $newContext->activeSpanId();
        static::assertNotNull($newActiveSpanId);
        static::assertTrue($newActiveSpanId->equals($spanId));
    }

    public function test_with_baggage_preserves_trace_id_and_active_span(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = Context::withTraceId($traceId)->withActiveSpan($spanId);

        $newContext = $context->withBaggage(new Baggage(['key' => 'value']));

        static::assertTrue($newContext->traceId->equals($traceId));
        $activeSpanId = $newContext->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertTrue($activeSpanId->equals($spanId));
    }

    public function test_with_baggage_replaces_baggage(): void
    {
        $context = new Context(TraceId::generate(), new Baggage(['old' => 'value']));
        $newBaggage = new Baggage(['new' => 'entry']);

        $newContext = $context->withBaggage($newBaggage);

        static::assertSame('value', $context->baggage->get('old'));
        static::assertNull($newContext->baggage->get('old'));
        static::assertSame('entry', $newContext->baggage->get('new'));
    }

    public function test_with_trace_id_creates_context_with_specific_trace_id(): void
    {
        $traceId = TraceId::generate();
        $context = Context::withTraceId($traceId);

        static::assertTrue($context->traceId->equals($traceId));
        static::assertTrue($context->baggage->isEmpty());
        static::assertNull($context->activeSpanId());
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_without_active_span_preserves_baggage(array $entries): void
    {
        $baggage = new Baggage($entries);
        $context = (new Context(TraceId::generate(), $baggage))->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        static::assertSame($entries, $newContext->baggage->all());
    }

    public function test_without_active_span_preserves_trace_id(): void
    {
        $traceId = TraceId::generate();
        $context = (new Context($traceId))->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        static::assertTrue($newContext->traceId->equals($traceId));
    }

    public function test_without_active_span_removes_span_id(): void
    {
        $context = Context::create()->withActiveSpan(SpanId::generate());

        $newContext = $context->withoutActiveSpan();

        static::assertNotNull($context->activeSpanId());
        static::assertNull($newContext->activeSpanId());
    }
}
