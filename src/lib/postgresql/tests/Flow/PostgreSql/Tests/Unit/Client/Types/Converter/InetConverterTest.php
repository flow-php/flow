<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\InetConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class InetConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new InetConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_integer_throws_exception() : void
    {
        $converter = new InetConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_ipv4_address() : void
    {
        $converter = new InetConverter();
        self::assertSame('192.168.1.1', $converter->toDatabase('192.168.1.1'));
    }

    public function test_ipv4_with_cidr() : void
    {
        $converter = new InetConverter();
        self::assertSame('192.168.1.1/24', $converter->toDatabase('192.168.1.1/24'));
    }

    public function test_ipv6_address() : void
    {
        $converter = new InetConverter();
        self::assertSame('::1', $converter->toDatabase('::1'));
    }

    public function test_null_handling() : void
    {
        $converter = new InetConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_to_database() : void
    {
        $converter = new InetConverter();
        $value = '10.0.0.1';

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('10.0.0.1', $dbValue);
    }

    public function test_supported_types() : void
    {
        $converter = new InetConverter();
        self::assertContains(PostgreSqlType::INET, $converter->supportedTypes());
    }
}
