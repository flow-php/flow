<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Serializer;

use Flow\Bridge\Telemetry\OTLP\Serializer\AttributeSerializer;
use Flow\Telemetry\Attributes;
use PHPUnit\Framework\TestCase;

final class AttributeSerializerTest extends TestCase
{
    private AttributeSerializer $serializer;

    protected function setUp(): void
    {
        $this->serializer = new AttributeSerializer();
    }

    public function test_serialize_array_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'tags' => ['foo', 'bar', 'baz'],
        ]));

        static::assertEquals(
            [
                [
                    'key' => 'tags',
                    'value' => [
                        'arrayValue' => [
                            'values' => [
                                ['stringValue' => 'foo'],
                                ['stringValue' => 'bar'],
                                ['stringValue' => 'baz'],
                            ],
                        ],
                    ],
                ],
            ],
            $result,
        );
    }

    public function test_serialize_boolean_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'enabled' => true,
            'disabled' => false,
        ]));

        static::assertEquals(
            [
                ['key' => 'enabled', 'value' => ['boolValue' => true]],
                ['key' => 'disabled', 'value' => ['boolValue' => false]],
            ],
            $result,
        );
    }

    public function test_serialize_empty_attributes(): void
    {
        $result = $this->serializer->serialize(Attributes::empty());

        static::assertSame([], $result);
    }

    public function test_serialize_float_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'duration' => 42.5,
            'ratio' => 0.75,
        ]));

        static::assertEquals(
            [
                ['key' => 'duration', 'value' => ['doubleValue' => 42.5]],
                ['key' => 'ratio', 'value' => ['doubleValue' => 0.75]],
            ],
            $result,
        );
    }

    public function test_serialize_integer_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'count' => 42,
            'port' => 8080,
        ]));

        static::assertEquals(
            [
                ['key' => 'count', 'value' => ['intValue' => '42']],
                ['key' => 'port', 'value' => ['intValue' => '8080']],
            ],
            $result,
        );
    }

    public function test_serialize_mixed_array_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'mixed' => [1, 'two', 3.0, true],
        ]));

        static::assertEquals(
            [
                [
                    'key' => 'mixed',
                    'value' => [
                        'arrayValue' => [
                            'values' => [
                                ['intValue' => '1'],
                                ['stringValue' => 'two'],
                                ['doubleValue' => 3.0],
                                ['boolValue' => true],
                            ],
                        ],
                    ],
                ],
            ],
            $result,
        );
    }

    public function test_serialize_string_values(): void
    {
        $result = $this->serializer->serialize(Attributes::create([
            'service.name' => 'my-service',
            'http.method' => 'GET',
        ]));

        static::assertEquals(
            [
                ['key' => 'service.name', 'value' => ['stringValue' => 'my-service']],
                ['key' => 'http.method', 'value' => ['stringValue' => 'GET']],
            ],
            $result,
        );
    }
}
