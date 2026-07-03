<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class ConsoleAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(ConsoleAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.symfony.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_attribute_keys_are_defined(): void
    {
        static::assertSame('flow.symfony.command.class', ConsoleAttributes::ATTR_COMMAND_CLASS);
        static::assertSame('flow.symfony.command.name', ConsoleAttributes::ATTR_COMMAND_NAME);
        static::assertSame('flow.symfony.command.signal', ConsoleAttributes::ATTR_COMMAND_SIGNAL);
    }
}
