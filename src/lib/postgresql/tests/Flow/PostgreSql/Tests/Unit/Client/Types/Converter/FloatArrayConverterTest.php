<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\FloatArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class FloatArrayConverterTest extends TestCase
{
    public function test_empty_array() : void
    {
        $converter = new FloatArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_float_array_to_database() : void
    {
        $converter = new FloatArrayConverter();
        $array = [1.5, 2.7, 3.9];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{1.5,2.7,3.9}', $dbValue);
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new FloatArrayConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([1.5, 'not a float', 3.9]);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new FloatArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new FloatArrayConverter();
        $array = [1.5, null, 3.9];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{1.5,NULL,3.9}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new FloatArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new FloatArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::FLOAT4_ARRAY, $types);
        self::assertContains(PostgreSqlType::FLOAT8_ARRAY, $types);
        self::assertCount(2, $types);
    }
}
