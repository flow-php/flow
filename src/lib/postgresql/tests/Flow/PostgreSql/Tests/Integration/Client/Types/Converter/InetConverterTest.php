<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_inet;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;

final class InetConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_inet_values(): \Generator
    {
        yield 'ipv4' => ['192.168.1.1', '192.168.1.1'];
        yield 'ipv4 with cidr' => ['192.168.1.1/24', '192.168.1.1/24'];
        yield 'ipv6 loopback' => ['::1', '::1'];
        yield 'ipv6' => ['2001:db8::1', '2001:db8::1'];
        yield 'ipv6 with cidr' => ['2001:db8::/32', '2001:db8::/32'];
    }

    #[DataProvider('provide_inet_values')]
    public function test_inet_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_inet())->as('val'))->toSql(), [typed(
                $input,
                ValueType::INET,
            )]);

        static::assertSame($expected, $result);
    }

    public function test_null_inet(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal(null), column_type_inet())->as('val'))->toSql());

        static::assertNull($result);
    }
}
