<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\HttpClientAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class HttpClientAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(HttpClientAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.http.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_attribute_keys_are_defined(): void
    {
        static::assertSame('flow.http.client.name', HttpClientAttributes::ATTR_CLIENT_NAME);
    }
}
