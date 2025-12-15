<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\JsonConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

final class JsonConverterTest extends TestCase
{
    public function test_array_conversion() : void
    {
        $converter = new JsonConverter();
        $array = ['name' => 'test', 'value' => 42, 'nested' => ['a' => 1]];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{"name":"test","value":42,"nested":{"a":1}}', $dbValue);
    }

    public function test_array_with_numeric_keys() : void
    {
        $converter = new JsonConverter();
        $array = ['a', 'b', 'c'];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('["a","b","c"]', $dbValue);
    }

    public function test_jsonb_type() : void
    {
        $converter = new JsonConverter();
        $result = $converter->toPhp('{"key":"value"}', PostgreSqlType::JSONB);
        self::assertInstanceOf(Json::class, $result);
        self::assertSame(['key' => 'value'], $result->toArray());
    }

    public function test_null_handling() : void
    {
        $converter = new JsonConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion_with_json_object() : void
    {
        $converter = new JsonConverter();
        $json = Json::fromString('{"name":"test","value":42}');

        $dbValue = $converter->toDatabase($json);
        self::assertNotNull($dbValue);
        self::assertSame('{"name":"test","value":42}', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::JSON);
        self::assertInstanceOf(Json::class, $phpValue);
        self::assertSame(['name' => 'test', 'value' => 42], $phpValue->toArray());
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

    public function test_to_php_returns_json_object() : void
    {
        $converter = new JsonConverter();
        $result = $converter->toPhp('{"key":"value"}', PostgreSqlType::JSON);
        self::assertInstanceOf(Json::class, $result);
    }
}
