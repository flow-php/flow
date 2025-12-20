<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\FloatConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class FloatConverterTest extends TestCase
{
    public function test_boolean_throws_exception() : void
    {
        $converter = new FloatConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(true);
    }

    public function test_float_to_database() : void
    {
        $converter = new FloatConverter();
        $value = 3.14159;

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('3.14159', $dbValue);
    }

    public function test_integer_conversion() : void
    {
        $converter = new FloatConverter();
        self::assertSame('42', $converter->toDatabase(42));
    }

    public function test_negative_float() : void
    {
        $converter = new FloatConverter();
        self::assertSame('-123.456', $converter->toDatabase(-123.456));
    }

    public function test_non_numeric_throws_exception() : void
    {
        $converter = new FloatConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_null_handling() : void
    {
        $converter = new FloatConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_conversion() : void
    {
        $converter = new FloatConverter();
        self::assertSame('3.14', $converter->toDatabase('3.14'));
    }

    public function test_supported_types() : void
    {
        $converter = new FloatConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::FLOAT4, $types);
        self::assertContains(PostgreSqlType::FLOAT8, $types);
        self::assertCount(2, $types);
    }

    public function test_zero() : void
    {
        $converter = new FloatConverter();
        self::assertSame('0', $converter->toDatabase(0.0));
    }
}
