<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\JsonConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class JsonConverterTest extends TestCase
{
    public function test_array_is_encoded_to_json() : void
    {
        $converter = new JsonConverter();
        $array = ['name' => 'test', 'value' => 42, 'nested' => ['a' => 1]];

        $result = $converter->toDatabase($array);
        self::assertSame('{"name":"test","value":42,"nested":{"a":1}}', $result);
    }

    public function test_invalid_type_throws_exception() : void
    {
        $converter = new JsonConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_json_string_to_database() : void
    {
        $converter = new JsonConverter();
        $json = '{"name":"test","value":42}';

        $dbValue = $converter->toDatabase($json);
        self::assertNotNull($dbValue);
        self::assertSame('{"name":"test","value":42}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new JsonConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_object_with_to_string_method() : void
    {
        $converter = new JsonConverter();
        $jsonObject = new class implements \Stringable {
            public function __toString() : string
            {
                return '{"name":"test","value":42}';
            }
        };

        $dbValue = $converter->toDatabase($jsonObject);
        self::assertSame('{"name":"test","value":42}', $dbValue);
    }

    public function test_string_json_passthrough() : void
    {
        $converter = new JsonConverter();
        self::assertSame('{"key":"value"}', $converter->toDatabase('{"key":"value"}'));
    }

    public function test_supported_types() : void
    {
        $converter = new JsonConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::JSON, $types);
        self::assertContains(PostgreSqlType::JSONB, $types);
    }
}
