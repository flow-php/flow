<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class InetConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_inet_values() : \Generator
    {
        yield 'ipv4' => ['192.168.1.1', '192.168.1.1'];
        yield 'ipv4 with cidr' => ['192.168.1.1/24', '192.168.1.1/24'];
        yield 'ipv6 loopback' => ['::1', '::1'];
        yield 'ipv6' => ['2001:db8::1', '2001:db8::1'];
        yield 'ipv6 with cidr' => ['2001:db8::/32', '2001:db8::/32'];
    }

    #[DataProvider('provide_inet_values')]
    public function test_inet_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::inet AS val', [typed($input, PostgreSqlType::INET)]);

        self::assertSame($expected, $result);
    }

    public function test_null_inet() : void
    {
        $result = $this->fetchValue('SELECT NULL::inet AS val');

        self::assertNull($result);
    }
}
