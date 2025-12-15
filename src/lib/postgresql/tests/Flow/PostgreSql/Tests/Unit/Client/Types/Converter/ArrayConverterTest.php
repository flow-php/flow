<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\ArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class ArrayConverterTest extends TestCase
{
    public function test_array_with_boolean_elements() : void
    {
        $converter = new ArrayConverter();
        $array = [true, false, true];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{t,f,t}', $dbValue);
    }

    public function test_array_with_empty_string_quoted() : void
    {
        $converter = new ArrayConverter();
        $array = ['', 'value'];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{"",value}', $dbValue);
    }

    public function test_array_with_null_element() : void
    {
        $converter = new ArrayConverter();
        $array = ['a', null, 'c'];

        $dbValue = $converter->toDatabase($array);
        self::assertNotNull($dbValue);
        self::assertSame('{a,NULL,c}', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::TEXT_ARRAY);
        self::assertSame(['a', null, 'c'], $phpValue);
    }

    public function test_array_with_special_characters_quoted() : void
    {
        $converter = new ArrayConverter();
        $array = ['hello world', 'with,comma', 'with"quote'];

        $dbValue = $converter->toDatabase($array);
        self::assertNotNull($dbValue);
        self::assertStringContainsString('"hello world"', $dbValue);
        self::assertStringContainsString('"with,comma"', $dbValue);
    }

    public function test_empty_array() : void
    {
        $converter = new ArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
        self::assertSame([], $converter->toPhp('{}', PostgreSqlType::TEXT_ARRAY));
    }

    public function test_integer_array() : void
    {
        $converter = new ArrayConverter();
        $array = [1, 2, 3];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{1,2,3}', $dbValue);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new ArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_handling() : void
    {
        $converter = new ArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_simple_array() : void
    {
        $converter = new ArrayConverter();
        $array = ['a', 'b', 'c'];

        $dbValue = $converter->toDatabase($array);
        self::assertNotNull($dbValue);
        self::assertSame('{a,b,c}', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::TEXT_ARRAY);
        self::assertSame(['a', 'b', 'c'], $phpValue);
    }

    public function test_supported_types() : void
    {
        $converter = new ArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::INT4_ARRAY, $types);
        self::assertContains(PostgreSqlType::TEXT_ARRAY, $types);
        self::assertContains(PostgreSqlType::VARCHAR_ARRAY, $types);
        self::assertContains(PostgreSqlType::UUID_ARRAY, $types);
        self::assertContains(PostgreSqlType::JSON_ARRAY, $types);
        self::assertContains(PostgreSqlType::JSONB_ARRAY, $types);
    }
}
