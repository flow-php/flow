<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\DbAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class DbAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(DbAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.db.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_attribute_keys_are_defined(): void
    {
        static::assertSame('flow.db.connection.name', DbAttributes::DB_CONNECTION_NAME);
        static::assertSame('flow.db.transaction.nesting_level', DbAttributes::DB_TRANSACTION_NESTING_LEVEL);
    }
}
