<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class MessengerAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(MessengerAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.messenger.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_attribute_keys_are_defined(): void
    {
        static::assertSame('flow.messenger.bus', MessengerAttributes::ATTR_BUS);
        static::assertSame('flow.messenger.message.class', MessengerAttributes::ATTR_MESSAGE_CLASS);
    }
}
