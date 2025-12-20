<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\JsonArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class JsonArrayConverterTest extends TestCase
{
    public function test_empty_array() : void
    {
        $converter = new JsonArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new JsonArrayConverter();

        $resource = \fopen('php://memory', 'rb');

        try {
            $this->expectException(ValueConversionException::class);
            $converter->toDatabase([['key' => 'value'], $resource]);
        } finally {
            \fclose($resource);
        }
    }

    public function test_json_array_to_database() : void
    {
        $converter = new JsonArrayConverter();
        $array = [
            ['key' => 'value1'],
            ['key' => 'value2'],
        ];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{"key":"value1"},{"key":"value2"}', \substr((string) $dbValue, 1, -1));
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new JsonArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new JsonArrayConverter();
        $array = [['key' => 'value'], null, ['key2' => 'value2']];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{{"key":"value"},NULL,{"key2":"value2"}}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new JsonArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new JsonArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::JSON_ARRAY, $types);
        self::assertContains(PostgreSqlType::JSONB_ARRAY, $types);
        self::assertCount(2, $types);
    }
}
