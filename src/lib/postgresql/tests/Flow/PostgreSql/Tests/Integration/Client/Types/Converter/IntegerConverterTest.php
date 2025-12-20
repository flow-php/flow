<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class IntegerConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{int, int}>
     */
    public static function provide_int2_values() : \Generator
    {
        yield 'zero' => [0, 0];
        yield 'positive' => [123, 123];
        yield 'negative' => [-456, -456];
        yield 'max int2' => [32767, 32767];
        yield 'min int2' => [-32768, -32768];
    }

    /**
     * @return \Generator<string, array{int, int}>
     */
    public static function provide_int4_values() : \Generator
    {
        yield 'zero' => [0, 0];
        yield 'positive' => [123456, 123456];
        yield 'negative' => [-789012, -789012];
        yield 'max int4' => [2147483647, 2147483647];
        yield 'min int4' => [-2147483648, -2147483648];
    }

    /**
     * @return \Generator<string, array{int, int}>
     */
    public static function provide_int8_values() : \Generator
    {
        yield 'zero' => [0, 0];
        yield 'positive large' => [9223372036854775000, 9223372036854775000];
        yield 'negative large' => [-9223372036854775000, -9223372036854775000];
    }

    #[DataProvider('provide_int2_values')]
    public function test_int2_round_trip(int $input, int $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::int2 AS val', [typed($input, PostgreSqlType::INT2)]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_int4_values')]
    public function test_int4_round_trip(int $input, int $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::int4 AS val', [typed($input, PostgreSqlType::INT4)]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_int8_values')]
    public function test_int8_round_trip(int $input, int $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::int8 AS val', [typed($input, PostgreSqlType::INT8)]);

        self::assertSame($expected, $result);
    }

    public function test_null_integer() : void
    {
        $result = $this->fetchValue('SELECT NULL::int4 AS val');

        self::assertNull($result);
    }
}
