<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use PHPUnit\Framework\Attributes\DataProvider;

final class FloatConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{float, float}>
     */
    public static function provide_float4_values() : \Generator
    {
        yield 'zero' => [0.0, 0.0];
        yield 'positive' => [3.14, 3.14];
        yield 'negative' => [-2.71, -2.71];
        yield 'small' => [0.0001, 0.0001];
    }

    /**
     * @return \Generator<string, array{float, float}>
     */
    public static function provide_float8_values() : \Generator
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
    public static function provide_numeric_values() : \Generator
    {
        yield 'zero' => ['0', '0'];
        yield 'integer' => ['12345', '12345'];
        yield 'decimal' => ['123.456', '123.456'];
        yield 'negative decimal' => ['-987.654', '-987.654'];
        yield 'high precision' => ['123456789.123456789', '123456789.123456789'];
    }

    #[DataProvider('provide_float4_values')]
    public function test_float4_round_trip(float $input, float $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::float4 AS val', [$input]);

        self::assertEqualsWithDelta($expected, $result, 0.0001);
    }

    #[DataProvider('provide_float8_values')]
    public function test_float8_round_trip(float $input, float $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::float8 AS val', [$input]);

        self::assertEqualsWithDelta($expected, $result, 0.00000001);
    }

    public function test_null_float() : void
    {
        $result = $this->fetchValue('SELECT NULL::float8 AS val');

        self::assertNull($result);
    }

    #[DataProvider('provide_numeric_values')]
    public function test_numeric_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::numeric AS val', [$input]);

        self::assertSame($expected, $result);
    }

    public function test_special_float_infinity() : void
    {
        $result = $this->fetchValue("SELECT 'Infinity'::float8 AS val");

        self::assertSame(INF, $result);
    }

    public function test_special_float_nan() : void
    {
        $result = $this->fetchValue("SELECT 'NaN'::float8 AS val");

        self::assertNan($result);
    }

    public function test_special_float_negative_infinity() : void
    {
        $result = $this->fetchValue("SELECT '-Infinity'::float8 AS val");

        self::assertSame(-INF, $result);
    }
}
