<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_double_precision;
use function Flow\PostgreSql\DSL\column_type_numeric;
use function Flow\PostgreSql\DSL\column_type_real;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;

final class FloatConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{float, float}>
     */
    public static function provide_float4_values(): \Generator
    {
        yield 'zero' => [0.0, 0.0];
        yield 'positive' => [3.14, 3.14];
        yield 'negative' => [-2.71, -2.71];
        yield 'small' => [0.0001, 0.0001];
    }

    /**
     * @return \Generator<string, array{float, float}>
     */
    public static function provide_float8_values(): \Generator
    {
        yield 'zero' => [0.0, 0.0];
        yield 'pi' => [3.14159265358979, 3.14159265358979];
        yield 'negative' => [-2.71828182845904, -2.71828182845904];
        yield 'large' => [1.7976931348623e+100, 1.7976931348623e+100];
        yield 'small' => [2.2250738585072e-100, 2.2250738585072e-100];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_numeric_values(): \Generator
    {
        yield 'zero' => ['0', '0'];
        yield 'integer' => ['12345', '12345'];
        yield 'decimal' => ['123.456', '123.456'];
        yield 'negative decimal' => ['-987.654', '-987.654'];
        yield 'high precision' => ['123456789.123456789', '123456789.123456789'];
    }

    #[DataProvider('provide_float4_values')]
    public function test_float4_round_trip(float $input, float $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_real())->as('val'))->toSql(), [typed(
                $input,
                ValueType::FLOAT4,
            )]);

        static::assertEqualsWithDelta($expected, $result, 0.0001);
    }

    #[DataProvider('provide_float8_values')]
    public function test_float8_round_trip(float $input, float $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_double_precision())->as('val'))->toSql(), [typed(
                $input,
                ValueType::FLOAT8,
            )]);

        static::assertEqualsWithDelta($expected, $result, 0.00000001);
    }

    public function test_null_float(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal(null), column_type_double_precision())->as('val'))->toSql());

        static::assertNull($result);
    }

    #[DataProvider('provide_numeric_values')]
    public function test_numeric_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(param(1), column_type_numeric())->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    public function test_special_float_infinity(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('Infinity'), column_type_double_precision())->as('val'))->toSql());

        static::assertSame(INF, $result);
    }

    public function test_special_float_nan(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('NaN'), column_type_double_precision())->as('val'))->toSql());

        static::assertNan($result);
    }

    public function test_special_float_negative_infinity(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalar(select(cast(literal('-Infinity'), column_type_double_precision())->as('val'))->toSql());

        static::assertSame(-INF, $result);
    }
}
