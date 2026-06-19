<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\SpanContext;
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
        static::assertNull(Context::root()->activeSpanId());
    }

    public function test_active_span_id_returns_span_id_when_set(): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $context = Context::root()->withActiveSpan($span);

        $activeSpanId = $context->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertTrue($activeSpanId->equals($span->spanId));
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_constructor_accepts_active_span_and_baggage(array $entries): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $context = new Context($span, new Baggage($entries));

        static::assertSame($span, $context->activeSpan());
        static::assertSame($entries, $context->baggage->all());
    }

    public function test_root_returns_context_without_active_span(): void
    {
        $context = Context::root();

        static::assertNull($context->activeSpan());
        static::assertNull($context->activeSpanId());
        static::assertNull($context->traceId());
        static::assertTrue($context->isRootContext());
        static::assertTrue($context->baggage->isEmpty());
    }

    public function test_trace_id_is_derived_from_active_span(): void
    {
        $traceId = TraceId::generate();
        $context = Context::root()->withActiveSpan(SpanContext::create($traceId, SpanId::generate()));

        $derived = $context->traceId();
        static::assertNotNull($derived);
        static::assertTrue($derived->equals($traceId));
    }

    public function test_default_baggage_is_empty(): void
    {
        static::assertTrue((new Context())->baggage->isEmpty());
    }

    public function test_from_array_creates_context_with_active_span(): void
    {
        $data = [
            'activeSpan' => [
                'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
                'spanId' => ['hex' => '00f067aa0ba902b7'],
                'parentSpanId' => null,
                'isRemote' => false,
            ],
            'baggage' => ['entries' => ['user.id' => '12345']],
        ];

        $context = Context::fromArray($data);

        $activeSpan = $context->activeSpan();
        static::assertNotNull($activeSpan);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $activeSpan->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $activeSpan->spanId->toHex());
        static::assertSame('12345', $context->baggage->get('user.id'));
    }

    public function test_from_array_creates_root_context(): void
    {
        $data = [
            'activeSpan' => null,
            'baggage' => ['entries' => ['user.id' => '12345']],
        ];

        $context = Context::fromArray($data);

        static::assertNull($context->activeSpan());
        static::assertSame('12345', $context->baggage->get('user.id'));
    }

    #[DataProvider('provideContextConfigurations')]
    public function test_is_root_context_based_on_active_span(bool $hasActiveSpan): void
    {
        $context = Context::root();

        if ($hasActiveSpan) {
            $context = $context->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        }

        static::assertSame(!$hasActiveSpan, $context->isRootContext());
    }

    public function test_normalize_from_array_round_trip_with_active_span(): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $original = (new Context(null, new Baggage(['a' => '1', 'b' => '2'])))->withActiveSpan($span);

        $restored = Context::fromArray($original->normalize());

        $originalSpan = $original->activeSpan();
        $restoredSpan = $restored->activeSpan();
        static::assertNotNull($originalSpan);
        static::assertNotNull($restoredSpan);
        static::assertTrue($originalSpan->traceId->equals($restoredSpan->traceId));
        static::assertTrue($originalSpan->spanId->equals($restoredSpan->spanId));
        static::assertSame($original->baggage->all(), $restored->baggage->all());
    }

    public function test_normalize_from_array_round_trip_root_context(): void
    {
        $original = new Context(null, new Baggage(['a' => '1', 'b' => '2']));

        $restored = Context::fromArray($original->normalize());

        static::assertNull($restored->activeSpan());
        static::assertSame($original->baggage->all(), $restored->baggage->all());
    }

    public function test_normalize_returns_array_with_active_span(): void
    {
        $context = (new Context(null, new Baggage(['user.id' => '12345'])))->withActiveSpan(SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        ));

        $normalized = $context->normalize();

        static::assertNotNull($normalized['activeSpan']);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['activeSpan']['traceId']['hex']);
        static::assertSame('00f067aa0ba902b7', $normalized['activeSpan']['spanId']['hex']);
        static::assertSame(['entries' => ['user.id' => '12345']], $normalized['baggage']);
    }

    public function test_normalize_returns_array_for_root_context(): void
    {
        $normalized = (new Context(null, new Baggage(['user.id' => '12345'])))->normalize();

        static::assertNull($normalized['activeSpan']);
        static::assertSame(['entries' => ['user.id' => '12345']], $normalized['baggage']);
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_with_active_span_preserves_baggage(array $entries): void
    {
        $context = new Context(null, new Baggage($entries));

        $newContext = $context->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));

        static::assertSame($entries, $newContext->baggage->all());
    }

    public function test_with_active_span_sets_span(): void
    {
        $context = Context::root();
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());

        $newContext = $context->withActiveSpan($span);

        static::assertNull($context->activeSpan());
        static::assertSame($span, $newContext->activeSpan());
    }

    public function test_with_baggage_preserves_active_span(): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $context = Context::root()->withActiveSpan($span);

        $newContext = $context->withBaggage(new Baggage(['key' => 'value']));

        static::assertSame($span, $newContext->activeSpan());
        static::assertSame('value', $newContext->baggage->get('key'));
    }

    public function test_with_baggage_replaces_baggage(): void
    {
        $context = new Context(null, new Baggage(['old' => 'value']));

        $newContext = $context->withBaggage(new Baggage(['new' => 'entry']));

        static::assertSame('value', $context->baggage->get('old'));
        static::assertNull($newContext->baggage->get('old'));
        static::assertSame('entry', $newContext->baggage->get('new'));
    }

    /**
     * @param array<string, string> $entries
     */
    #[DataProvider('provideBaggageEntries')]
    public function test_without_active_span_preserves_baggage(array $entries): void
    {
        $context = (new Context(null, new Baggage($entries)))->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        ));

        static::assertSame($entries, $context->withoutActiveSpan()->baggage->all());
    }

    public function test_without_active_span_removes_span(): void
    {
        $context = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));

        $newContext = $context->withoutActiveSpan();

        static::assertNotNull($context->activeSpan());
        static::assertNull($newContext->activeSpan());
    }
}
