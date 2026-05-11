<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_cidr;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;

final class CidrConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_cidr_values(): \Generator
    {
        yield 'ipv4 /8' => ['10.0.0.0/8', '10.0.0.0/8'];
        yield 'ipv4 /16' => ['172.16.0.0/16', '172.16.0.0/16'];
        yield 'ipv4 /24' => ['192.168.1.0/24', '192.168.1.0/24'];
        yield 'ipv6' => ['2001:db8::/32', '2001:db8::/32'];
    }

    #[DataProvider('provide_cidr_values')]
    public function test_cidr_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_cidr())->as('val'))->toSql(), [typed(
                $input,
                ValueType::CIDR,
            )]);

        static::assertSame($expected, $result);
    }

    public function test_null_cidr(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal(null), column_type_cidr())->as('val'))->toSql());

        static::assertNull($result);
    }
}
