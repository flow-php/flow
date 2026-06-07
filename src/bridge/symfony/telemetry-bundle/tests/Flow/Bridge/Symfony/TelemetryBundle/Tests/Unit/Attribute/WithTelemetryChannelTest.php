<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Attribute;

use Attribute;
use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

#[CoversClass(WithTelemetryChannel::class)]
final class WithTelemetryChannelTest extends TestCase
{
    public function test_exposes_the_channel(): void
    {
        static::assertSame('events', (new WithTelemetryChannel('events'))->channel);
    }

    public function test_is_a_class_targeted_attribute(): void
    {
        $attributes = (new ReflectionClass(WithTelemetryChannel::class))->getAttributes(Attribute::class);

        static::assertCount(1, $attributes);
        static::assertSame(Attribute::TARGET_CLASS, $attributes[0]->newInstance()->flags);
    }
}
