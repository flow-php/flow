<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\IntegerConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class IntegerConverterTest extends TestCase
{
    public function test_large_int8_value() : void
    {
        $converter = new IntegerConverter();
        $largeValue = 9223372036854775807;

        $dbValue = $converter->toDatabase($largeValue);
        self::assertNotNull($dbValue);
        self::assertSame('9223372036854775807', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::INT8);
        self::assertSame($largeValue, $phpValue);
    }

    public function test_negative_integer() : void
    {
        $converter = new IntegerConverter();
        self::assertSame('-123', $converter->toDatabase(-123));
        self::assertSame(-123, $converter->toPhp('-123', PostgreSqlType::INT4));
    }

    public function test_non_scalar_returns_zero() : void
    {
        $converter = new IntegerConverter();
        self::assertSame('0', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new IntegerConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new IntegerConverter();
        $value = 42;

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('42', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::INT4);
        self::assertSame($value, $phpValue);
    }

    public function test_scalar_conversion() : void
    {
        $converter = new IntegerConverter();
        self::assertSame('3', $converter->toDatabase(3.7));
        self::assertSame('1', $converter->toDatabase(true));
        self::assertSame('0', $converter->toDatabase(false));
        self::assertSame('42', $converter->toDatabase('42'));
    }

    public function test_supported_types() : void
    {
        $converter = new IntegerConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::INT2, $types);
        self::assertContains(PostgreSqlType::INT4, $types);
        self::assertContains(PostgreSqlType::INT8, $types);
    }

    public function test_zero() : void
    {
        $converter = new IntegerConverter();
        self::assertSame('0', $converter->toDatabase(0));
        self::assertSame(0, $converter->toPhp('0', PostgreSqlType::INT4));
    }
}
