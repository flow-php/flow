<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\FloatConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class FloatConverterTest extends TestCase
{
    public function test_integer_conversion() : void
    {
        $converter = new FloatConverter();
        self::assertSame('42', $converter->toDatabase(42));
        self::assertSame(42.0, $converter->toPhp('42', PostgreSqlType::FLOAT8));
    }

    public function test_negative_float() : void
    {
        $converter = new FloatConverter();
        self::assertSame('-123.456', $converter->toDatabase(-123.456));
        self::assertSame(-123.456, $converter->toPhp('-123.456', PostgreSqlType::FLOAT8));
    }

    public function test_non_scalar_returns_zero() : void
    {
        $converter = new FloatConverter();
        self::assertSame('0', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new FloatConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new FloatConverter();
        $value = 3.14159;

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('3.14159', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::FLOAT8);
        self::assertSame($value, $phpValue);
    }

    public function test_scalar_conversion() : void
    {
        $converter = new FloatConverter();
        self::assertSame('1', $converter->toDatabase(true));
        self::assertSame('3.14', $converter->toDatabase('3.14'));
    }

    public function test_scientific_notation() : void
    {
        $converter = new FloatConverter();
        self::assertSame(1.23E-10, $converter->toPhp('1.23E-10', PostgreSqlType::FLOAT8));
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
        self::assertSame(0.0, $converter->toPhp('0', PostgreSqlType::FLOAT8));
    }
}
