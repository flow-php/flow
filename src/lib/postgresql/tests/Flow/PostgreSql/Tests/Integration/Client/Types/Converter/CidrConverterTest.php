<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class CidrConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_cidr_values() : \Generator
    {
        yield 'ipv4 /8' => ['10.0.0.0/8', '10.0.0.0/8'];
        yield 'ipv4 /16' => ['172.16.0.0/16', '172.16.0.0/16'];
        yield 'ipv4 /24' => ['192.168.1.0/24', '192.168.1.0/24'];
        yield 'ipv6' => ['2001:db8::/32', '2001:db8::/32'];
    }

    #[DataProvider('provide_cidr_values')]
    public function test_cidr_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::cidr AS val', [typed($input, PostgreSqlType::CIDR)]);

        self::assertSame($expected, $result);
    }

    public function test_null_cidr() : void
    {
        $result = $this->fetchValue('SELECT NULL::cidr AS val');

        self::assertNull($result);
    }
}
