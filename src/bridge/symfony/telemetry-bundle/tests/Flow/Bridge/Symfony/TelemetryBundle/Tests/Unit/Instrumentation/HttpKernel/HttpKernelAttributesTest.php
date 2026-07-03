<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class HttpKernelAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(HttpKernelAttributes::class))->getConstants() as $name => $value) {
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
        static::assertSame('flow.symfony.controller', HttpKernelAttributes::ATTR_CONTROLLER);
        static::assertSame('flow.symfony.controller.argument', HttpKernelAttributes::ATTR_CONTROLLER_ARGUMENT);
    }
}
