<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\MoneyConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class MoneyConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new MoneyConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_float_throws_exception() : void
    {
        $converter = new MoneyConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(123.45);
    }

    public function test_integer_throws_exception() : void
    {
        $converter = new MoneyConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(100);
    }

    public function test_null_handling() : void
    {
        $converter = new MoneyConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_passthrough() : void
    {
        $converter = new MoneyConverter();
        self::assertSame('$99.99', $converter->toDatabase('$99.99'));
    }

    public function test_string_to_database() : void
    {
        $converter = new MoneyConverter();
        $value = '$1,234.56';

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('$1,234.56', $dbValue);
    }

    public function test_supported_types() : void
    {
        $converter = new MoneyConverter();
        self::assertContains(PostgreSqlType::MONEY, $converter->supportedTypes());
    }
}
