<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\SpanLimits;
use PHPUnit\Framework\TestCase;

final class SpanLimitsTest extends TestCase
{
    public function test_constructor_with_custom_values() : void
    {
        $limits = new SpanLimits(
            attributeCountLimit: 64,
            eventCountLimit: 32,
            linkCountLimit: 16,
            attributePerEventCountLimit: 8,
            attributePerLinkCountLimit: 4,
            attributeValueLengthLimit: 1024,
        );

        self::assertSame(64, $limits->attributeCountLimit);
        self::assertSame(32, $limits->eventCountLimit);
        self::assertSame(16, $limits->linkCountLimit);
        self::assertSame(8, $limits->attributePerEventCountLimit);
        self::assertSame(4, $limits->attributePerLinkCountLimit);
        self::assertSame(1024, $limits->attributeValueLengthLimit);
    }

    public function test_constructor_with_partial_custom_values() : void
    {
        $limits = new SpanLimits(
            attributeCountLimit: 256,
        );

        self::assertSame(256, $limits->attributeCountLimit);
        self::assertSame(128, $limits->eventCountLimit);
        self::assertSame(128, $limits->linkCountLimit);
    }

    public function test_default_creates_limits_with_default_values() : void
    {
        $limits = SpanLimits::default();

        self::assertSame(128, $limits->attributeCountLimit);
        self::assertSame(128, $limits->eventCountLimit);
        self::assertSame(128, $limits->linkCountLimit);
        self::assertSame(128, $limits->attributePerEventCountLimit);
        self::assertSame(128, $limits->attributePerLinkCountLimit);
        self::assertNull($limits->attributeValueLengthLimit);
    }

    public function test_unlimited_creates_limits_with_max_values() : void
    {
        $limits = SpanLimits::unlimited();

        self::assertSame(PHP_INT_MAX, $limits->attributeCountLimit);
        self::assertSame(PHP_INT_MAX, $limits->eventCountLimit);
        self::assertSame(PHP_INT_MAX, $limits->linkCountLimit);
        self::assertSame(PHP_INT_MAX, $limits->attributePerEventCountLimit);
        self::assertSame(PHP_INT_MAX, $limits->attributePerLinkCountLimit);
        self::assertNull($limits->attributeValueLengthLimit);
    }
}
