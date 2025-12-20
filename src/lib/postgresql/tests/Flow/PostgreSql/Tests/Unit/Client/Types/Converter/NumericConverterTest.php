<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\NumericConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class NumericConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new NumericConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_float_conversion() : void
    {
        $converter = new NumericConverter();
        self::assertSame('3.14159', $converter->toDatabase(3.14159));
    }

    public function test_integer_conversion() : void
    {
        $converter = new NumericConverter();
        self::assertSame('42', $converter->toDatabase(42));
    }

    public function test_large_precision_string() : void
    {
        $converter = new NumericConverter();
        $largeValue = '12345678901234567890.12345678901234567890';
        self::assertSame($largeValue, $converter->toDatabase($largeValue));
    }

    public function test_null_handling() : void
    {
        $converter = new NumericConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_passthrough() : void
    {
        $converter = new NumericConverter();
        self::assertSame('999.99', $converter->toDatabase('999.99'));
    }

    public function test_supported_types() : void
    {
        $converter = new NumericConverter();
        self::assertContains(PostgreSqlType::NUMERIC, $converter->supportedTypes());
    }
}
