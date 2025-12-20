<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class MoneyConverterTest extends ConverterTestCase
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
        $result = $this->fetchValue('SELECT 99.99::money AS val');

        self::assertSame('$99.99', $result);
    }

    #[DataProvider('provide_money_values')]
    public function test_money_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::money AS val', [typed($input, PostgreSqlType::MONEY)]);

        self::assertSame($expected, $result);
    }

    public function test_null_money() : void
    {
        $result = $this->fetchValue('SELECT NULL::money AS val');

        self::assertNull($result);
    }
}
