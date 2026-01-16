<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Tracer\{SpanContext, SpanLink};
use PHPUnit\Framework\TestCase;

final class SpanLinkTest extends TestCase
{
    public function test_constructor_creates_link() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $attributes = Attributes::create(['reason' => 'batch']);

        $link = new SpanLink($context, $attributes);

        self::assertSame($context, $link->context);
        self::assertSame($attributes, $link->attributes);
    }

    public function test_constructor_creates_link_with_empty_attributes() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $link = new SpanLink($context);

        self::assertSame([], $link->attributes->normalize());
    }

    public function test_create_creates_link() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $attributes = ['type' => 'fan-out'];

        $link = SpanLink::create($context, $attributes);

        self::assertSame($context, $link->context);
        self::assertSame($attributes, $link->attributes->normalize());
    }

    public function test_create_creates_link_with_empty_attributes() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $link = SpanLink::create($context);

        self::assertSame([], $link->attributes->normalize());
    }

    public function test_from_array_creates_link() : void
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

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $link->context->traceId->toHex());
        self::assertSame('00f067aa0ba902b7', $link->context->spanId->toHex());
        self::assertSame(['restored' => 'link'], $link->attributes->normalize());
    }

    public function test_from_array_creates_link_with_parent_span() : void
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

        self::assertNotNull($link->context->parentSpanId);
        self::assertSame('11f067aa0ba902b8', $link->context->parentSpanId->toHex());
        self::assertTrue($link->context->isRemote);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $originalContext = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            SpanId::generate(),
        );
        $original = SpanLink::create($originalContext, ['key' => 'value']);

        $normalized = $original->normalize();
        $restored = SpanLink::fromArray($normalized);

        self::assertTrue($original->context->traceId->equals($restored->context->traceId));
        self::assertTrue($original->context->spanId->equals($restored->context->spanId));
        self::assertNotNull($original->context->parentSpanId);
        self::assertNotNull($restored->context->parentSpanId);
        self::assertTrue($original->context->parentSpanId->equals($restored->context->parentSpanId));
        self::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
    }

    public function test_normalize_returns_array() : void
    {
        $context = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
        );
        $link = SpanLink::create($context, ['type' => 'test']);

        $normalized = $link->normalize();

        self::assertSame('0af7651916cd43dd8448eb211c80319c', $normalized['context']['traceId']['hex']);
        self::assertSame('00f067aa0ba902b7', $normalized['context']['spanId']['hex']);
        self::assertNull($normalized['context']['parentSpanId']);
        self::assertFalse($normalized['context']['isRemote']);
        self::assertSame(['type' => 'test'], $normalized['attributes']);
    }

    public function test_supports_various_attribute_types() : void
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

        self::assertSame($attributes, $link->attributes->normalize());
    }
}
