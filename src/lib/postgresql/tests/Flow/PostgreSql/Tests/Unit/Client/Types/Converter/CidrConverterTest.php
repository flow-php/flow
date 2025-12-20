<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\CidrConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class CidrConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new CidrConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_integer_throws_exception() : void
    {
        $converter = new CidrConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_ipv4_network() : void
    {
        $converter = new CidrConverter();
        self::assertSame('192.168.1.0/24', $converter->toDatabase('192.168.1.0/24'));
    }

    public function test_ipv6_network() : void
    {
        $converter = new CidrConverter();
        self::assertSame('2001:db8::/32', $converter->toDatabase('2001:db8::/32'));
    }

    public function test_null_handling() : void
    {
        $converter = new CidrConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_to_database() : void
    {
        $converter = new CidrConverter();
        $value = '10.0.0.0/8';

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('10.0.0.0/8', $dbValue);
    }

    public function test_supported_types() : void
    {
        $converter = new CidrConverter();
        self::assertContains(PostgreSqlType::CIDR, $converter->supportedTypes());
    }
}
