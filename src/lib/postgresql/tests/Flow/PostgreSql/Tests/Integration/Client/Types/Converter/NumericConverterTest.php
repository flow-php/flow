<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_numeric, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class NumericConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_numeric_values() : \Generator
    {
        yield 'zero' => ['0', '0'];
        yield 'positive integer' => ['123', '123'];
        yield 'negative integer' => ['-456', '-456'];
        yield 'decimal' => ['123.456', '123.456'];
        yield 'negative decimal' => ['-789.012', '-789.012'];
        yield 'large precision' => ['12345678901234567890.12345678901234567890', '12345678901234567890.12345678901234567890'];
    }

    public function test_null_numeric() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_numeric())->as('val'))->toSql());

        self::assertNull($result);
    }

    #[DataProvider('provide_numeric_values')]
    public function test_numeric_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_numeric())->as('val'))->toSql(), [typed($input, ValueType::NUMERIC)]);

        self::assertSame($expected, $result);
    }

    public function test_numeric_with_precision_and_scale() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_numeric(10, 2))->as('val'))->toSql(), [typed('1234.5678', ValueType::NUMERIC)]);

        self::assertSame('1234.57', $result);
    }
}
