<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\CidrConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

final class CidrConverterTest extends TestCase
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
        yield 'IPv4 network /8' => ['10.0.0.0/8', '10.0.0.0/8'];
        yield 'IPv4 network /16' => ['172.16.0.0/16', '172.16.0.0/16'];
        yield 'IPv4 network /24' => ['192.168.1.0/24', '192.168.1.0/24'];
        yield 'IPv4 host /32' => ['192.168.1.1/32', '192.168.1.1/32'];
        yield 'IPv4 default /0' => ['0.0.0.0/0', '0.0.0.0/0'];
        yield 'IPv4 loopback network' => ['127.0.0.0/8', '127.0.0.0/8'];
        yield 'IPv4 private class A' => ['10.0.0.0/8', '10.0.0.0/8'];
        yield 'IPv4 private class B' => ['172.16.0.0/12', '172.16.0.0/12'];
        yield 'IPv4 private class C' => ['192.168.0.0/16', '192.168.0.0/16'];
        yield 'IPv6 network /32' => ['2001:db8::/32', '2001:db8::/32'];
        yield 'IPv6 network /64' => ['2001:db8:85a3::/64', '2001:db8:85a3::/64'];
        yield 'IPv6 host /128' => ['::1/128', '::1/128'];
        yield 'IPv6 default /0' => ['::/0', '::/0'];
        yield 'IPv6 link-local' => ['fe80::/10', 'fe80::/10'];
        yield 'IPv6 multicast' => ['ff00::/8', 'ff00::/8'];
        yield 'IPv6 loopback' => ['::1/128', '::1/128'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new CidrConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new CidrConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new CidrConverter();
        static::assertContains(ValueType::CIDR, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected): void
    {
        $converter = new CidrConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
