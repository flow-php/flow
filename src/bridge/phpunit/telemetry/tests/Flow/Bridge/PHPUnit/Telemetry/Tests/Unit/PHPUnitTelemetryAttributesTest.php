<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\PHPUnitTelemetryAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class PHPUnitTelemetryAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_phpunit_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(PHPUnitTelemetryAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.phpunit.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_suite_attributes_are_defined(): void
    {
        static::assertSame('flow.phpunit.suite.is_root', PHPUnitTelemetryAttributes::ATTR_SUITE_IS_ROOT);
        static::assertSame('flow.phpunit.suite.test_count', PHPUnitTelemetryAttributes::ATTR_SUITE_TEST_COUNT);
    }

    public function test_test_attributes_are_defined(): void
    {
        static::assertSame('flow.phpunit.test.class', PHPUnitTelemetryAttributes::ATTR_TEST_CLASS);
        static::assertSame('flow.phpunit.test.id', PHPUnitTelemetryAttributes::ATTR_TEST_ID);
        static::assertSame('flow.phpunit.test.memory.delta', PHPUnitTelemetryAttributes::ATTR_TEST_MEMORY_DELTA);
        static::assertSame('flow.phpunit.test.memory.peak', PHPUnitTelemetryAttributes::ATTR_TEST_MEMORY_PEAK);
        static::assertSame('flow.phpunit.test.method', PHPUnitTelemetryAttributes::ATTR_TEST_METHOD);
    }
}
