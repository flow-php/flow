<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\InetConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

final class InetConverterTest extends TestCase
{
    public static function provide_invalid_values(): Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new stdClass()];
    }

    public static function provide_valid_values(): Generator
    {
        yield 'IPv4 address' => ['192.168.1.1', '192.168.1.1'];
        yield 'IPv4 with CIDR' => ['192.168.1.1/24', '192.168.1.1/24'];
        yield 'IPv4 simple' => ['10.0.0.1', '10.0.0.1'];
        yield 'IPv4 loopback' => ['127.0.0.1', '127.0.0.1'];
        yield 'IPv4 broadcast' => ['255.255.255.255', '255.255.255.255'];
        yield 'IPv4 any' => ['0.0.0.0', '0.0.0.0'];
        yield 'IPv4 host /32' => ['192.168.1.1/32', '192.168.1.1/32'];
        yield 'IPv6 loopback' => ['::1', '::1'];
        yield 'IPv6 any' => ['::', '::'];
        yield 'IPv6 full' => ['2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8:85a3:0000:0000:8a2e:0370:7334'];
        yield 'IPv6 compressed' => ['2001:db8:85a3::8a2e:370:7334', '2001:db8:85a3::8a2e:370:7334'];
        yield 'IPv6 with CIDR' => ['2001:db8::/32', '2001:db8::/32'];
        yield 'IPv6 host /128' => ['::1/128', '::1/128'];
        yield 'IPv4-mapped IPv6' => ['::ffff:192.168.1.1', '::ffff:192.168.1.1'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new InetConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new InetConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new InetConverter();
        static::assertContains(ValueType::INET, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected): void
    {
        $converter = new InetConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
