<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_bigint, column_type_integer, column_type_smallint, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class IntegerConverterTest extends PostgreSqlTestCase
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
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_smallint())->as('val'))->toSql(), [typed($input, ValueType::INT2)]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_int4_values')]
    public function test_int4_round_trip(int $input, int $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_integer())->as('val'))->toSql(), [typed($input, ValueType::INT4)]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_int8_values')]
    public function test_int8_round_trip(int $input, int $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_bigint())->as('val'))->toSql(), [typed($input, ValueType::INT8)]);

        self::assertSame($expected, $result);
    }

    public function test_null_integer() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_integer())->as('val'))->toSql());

        self::assertNull($result);
    }
}
