<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_custom, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class MoneyConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_money_values() : \Generator
    {
        yield 'zero' => ['$0.00', '$0.00'];
        yield 'positive' => ['$1,234.56', '$1,234.56'];
        yield 'negative' => ['-$789.00', '-$789.00'];
    }

    public function test_money_from_numeric() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(99.99), column_type_custom('money'))->as('val'))->toSql());

        self::assertSame('$99.99', $result);
    }

    #[DataProvider('provide_money_values')]
    public function test_money_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_custom('money'))->as('val'))->toSql(), [typed($input, ValueType::MONEY)]);

        self::assertSame($expected, $result);
    }

    public function test_null_money() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_custom('money'))->as('val'))->toSql());

        self::assertNull($result);
    }
}
