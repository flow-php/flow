<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\SpanLimits;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\span_limits;

final class SpanLimitsTest extends TestCase
{
    public function test_constructor_with_custom_values(): void
    {
        $limits = new SpanLimits(
            attributeCountLimit: 64,
            eventCountLimit: 32,
            linkCountLimit: 16,
            attributePerEventCountLimit: 8,
            attributePerLinkCountLimit: 4,
            attributeValueLengthLimit: 1024,
        );

        static::assertSame(64, $limits->attributeCountLimit);
        static::assertSame(32, $limits->eventCountLimit);
        static::assertSame(16, $limits->linkCountLimit);
        static::assertSame(8, $limits->attributePerEventCountLimit);
        static::assertSame(4, $limits->attributePerLinkCountLimit);
        static::assertSame(1024, $limits->attributeValueLengthLimit);
    }

    public function test_constructor_with_partial_custom_values(): void
    {
        $limits = new SpanLimits(attributeCountLimit: 256);

        static::assertSame(256, $limits->attributeCountLimit);
        static::assertSame(128, $limits->eventCountLimit);
        static::assertSame(128, $limits->linkCountLimit);
    }

    public function test_default_creates_limits_with_default_values(): void
    {
        $limits = SpanLimits::default();

        static::assertSame(128, $limits->attributeCountLimit);
        static::assertSame(128, $limits->eventCountLimit);
        static::assertSame(128, $limits->linkCountLimit);
        static::assertSame(128, $limits->attributePerEventCountLimit);
        static::assertSame(128, $limits->attributePerLinkCountLimit);
        static::assertNull($limits->attributeValueLengthLimit);
    }

    public function test_unlimited_creates_limits_with_max_values(): void
    {
        $limits = span_limits()->unlimited();

        static::assertSame(PHP_INT_MAX, $limits->attributeCountLimit);
        static::assertSame(PHP_INT_MAX, $limits->eventCountLimit);
        static::assertSame(PHP_INT_MAX, $limits->linkCountLimit);
        static::assertSame(PHP_INT_MAX, $limits->attributePerEventCountLimit);
        static::assertSame(PHP_INT_MAX, $limits->attributePerLinkCountLimit);
        static::assertNull($limits->attributeValueLengthLimit);
    }
}
