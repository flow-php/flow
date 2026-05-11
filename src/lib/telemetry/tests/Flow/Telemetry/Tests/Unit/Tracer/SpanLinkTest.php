<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanLink;
use PHPUnit\Framework\TestCase;

final class SpanLinkTest extends TestCase
{
    public function test_constructor_creates_link(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $attributes = Attributes::create(['reason' => 'batch']);

        $link = new SpanLink($context, $attributes);

        static::assertSame($context, $link->context);
        static::assertSame($attributes, $link->attributes);
    }

    public function test_constructor_creates_link_with_empty_attributes(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $link = new SpanLink($context);

        static::assertSame([], $link->attributes->normalize());
    }

    public function test_create_creates_link(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $attributes = ['type' => 'fan-out'];

        $link = SpanLink::create($context, $attributes);

        static::assertSame($context, $link->context);
        static::assertSame($attributes, $link->attributes->normalize());
    }

    public function test_create_creates_link_with_empty_attributes(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $link = SpanLink::create($context);

        static::assertSame([], $link->attributes->normalize());
    }

    public function test_from_array_creates_link(): void
    {
        $data = [
            'context' => [
                'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
                'spanId' => ['hex' => '00f067aa0ba902b7'],
                'parentSpanId' => null,
                'isRemote' => false,
            ],
            'attributes' => ['restored' => 'link'],
        ];

        $link = SpanLink::fromArray($data);

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $link->context->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $link->context->spanId->toHex());
        static::assertSame(['restored' => 'link'], $link->attributes->normalize());
    }

    public function test_from_array_creates_link_with_parent_span(): void
    {
        $data = [
            'context' => [
                'traceId' => ['hex' => '0af7651916cd43dd8448eb211c80319c'],
                'spanId' => ['hex' => '00f067aa0ba902b7'],
                'parentSpanId' => ['hex' => '11f067aa0ba902b8'],
                'isRemote' => true,
            ],
            'attributes' => [],
        ];

        $link = SpanLink::fromArray($data);

        static::assertNotNull($link->context->parentSpanId);
        static::assertSame('11f067aa0ba902b8', $link->context->parentSpanId->toHex());
        static::assertTrue($link->context->isRemote);
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $originalContext = SpanContext::create(TraceId::generate(), SpanId::generate(), SpanId::generate());
        $original = SpanLink::create($originalContext, ['key' => 'value']);

        $normalized = $original->normalize();
        $restored = SpanLink::fromArray($normalized);

        static::assertTrue($original->context->traceId->equals($restored->context->traceId));
        static::assertTrue($original->context->spanId->equals($restored->context->spanId));
        static::assertNotNull($original->context->parentSpanId);
        static::assertNotNull($restored->context->parentSpanId);
        static::assertTrue($original->context->parentSpanId->equals($restored->context->parentSpanId));
        static::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
    }

    public function test_normalize_returns_array(): void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $link = SpanLink::create($context, ['type' => 'test']);

        $normalized = $link->normalize();

        static::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['context']['traceId']['hex']);
        static::assertSame('00f067aa0ba902b7', $normalized['context']['spanId']['hex']);
        static::assertNull($normalized['context']['parentSpanId']);
        static::assertFalse($normalized['context']['isRemote']);
        static::assertSame(['type' => 'test'], $normalized['attributes']);
        static::assertSame(0, $normalized['droppedAttributeCount']);
    }

    public function test_supports_various_attribute_types(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $attributes = [
            'string' => 'text',
            'int' => 42,
            'float' => 3.14,
            'bool' => true,
            'array' => ['a', 'b', 'c'],
        ];

        $link = SpanLink::create($context, $attributes);

        static::assertSame($attributes, $link->attributes->normalize());
    }
}
