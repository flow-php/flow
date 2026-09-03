<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\ResultCaster;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use const INF;
use const PHP_INT_SIZE;

final class ResultCasterTest extends TestCase
{
    private ResultCaster $caster;

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_float_types(): Generator
    {
        yield 'float4' => ['float4'];
        yield 'float8' => ['float8'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_small_integer_types(): Generator
    {
        yield 'int2' => ['int2'];
        yield 'int4' => ['int4'];
    }

    /**
     * pg hands these to PHP as text and the schema types them as strings, so the caster must
     * leave them exactly as they arrive.
     *
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_text_remainder_types(): Generator
    {
        yield 'money' => ['money', '$12.50'];
        yield 'interval' => ['interval', '2 years 3 mons'];
        yield 'inet' => ['inet', '192.168.1.1'];
        yield 'cidr' => ['cidr', '10.0.0.0/8'];
        yield 'macaddr' => ['macaddr', '08:00:2b:01:02:03'];
        yield 'int4range' => ['int4range', '[1,10)'];
        yield 'numrange' => ['numrange', '[1.5,2.5)'];
        yield 'daterange' => ['daterange', '[2026-01-01,2026-02-01)'];
        yield 'a pg enum reports its own type name' => ['mood', 'happy'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_string_types(): Generator
    {
        yield 'text' => ['text'];
        yield 'varchar' => ['varchar'];
        yield 'json' => ['json'];
        yield 'jsonb' => ['jsonb'];
        yield 'uuid' => ['uuid'];
        yield 'date' => ['date'];
        yield 'timestamptz' => ['timestamptz'];
        yield 'time' => ['time'];
        yield 'numeric' => ['numeric'];
        yield 'money' => ['money'];
        yield 'inet' => ['inet'];
        yield 'cidr' => ['cidr'];
    }

    protected function setUp(): void
    {
        $this->caster = new ResultCaster();
    }

    public function test_bool_false(): void
    {
        static::assertFalse($this->caster->cast('f', 'bool'));
    }

    public function test_bool_true(): void
    {
        static::assertTrue($this->caster->cast('t', 'bool'));
    }

    public function test_an_array_element_null_stays_null(): void
    {
        static::assertSame([null, 'a'], $this->caster->cast('{NULL,a}', '_text'));
    }

    public function test_an_array_type_is_parsed_into_a_php_array(): void
    {
        static::assertSame([1, 2], $this->caster->cast('{1,2}', '_int4'));
        static::assertSame(['a', 'b'], $this->caster->cast('{a,b}', '_text'));
        static::assertSame([true, false], $this->caster->cast('{t,f}', '_bool'));
    }

    public function test_a_nested_array_is_cast_element_wise(): void
    {
        static::assertSame([[1, 2], [3]], $this->caster->cast('{{1,2},{3}}', '_int4'));
    }

    public function test_bytea_decodes_hex(): void
    {
        static::assertSame('hello', $this->caster->cast('\x68656c6c6f', 'bytea'));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_infinity(string $type): void
    {
        static::assertSame(INF, $this->caster->cast('Infinity', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_nan(string $type): void
    {
        static::assertNan($this->caster->cast('NaN', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_negative_infinity(string $type): void
    {
        static::assertSame(-INF, $this->caster->cast('-Infinity', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_types(string $type): void
    {
        static::assertSame(3.14, $this->caster->cast('3.14', $type));
        static::assertSame(-0.5, $this->caster->cast('-0.5', $type));
        static::assertSame(0.0, $this->caster->cast('0', $type));
    }

    public function test_int8_on_64bit_platform(): void
    {
        if (PHP_INT_SIZE < 8) {
            static::markTestSkipped('Test requires 64-bit PHP');
        }

        static::assertSame(9223372036854775807, $this->caster->cast('9223372036854775807', 'int8'));
        static::assertSame(42, $this->caster->cast('42', 'int8'));
    }

    public function test_null_type_returns_string(): void
    {
        static::assertSame('some value', $this->caster->cast('some value', null));
    }

    #[DataProvider('provide_small_integer_types')]
    public function test_small_integer_types(string $type): void
    {
        static::assertSame(42, $this->caster->cast('42', $type));
        static::assertSame(-100, $this->caster->cast('-100', $type));
        static::assertSame(0, $this->caster->cast('0', $type));
    }

    #[DataProvider('provide_string_types')]
    public function test_string_types_pass_through(string $type): void
    {
        $value = 'test string value';
        static::assertSame($value, $this->caster->cast($value, $type));
    }

    #[DataProvider('provide_text_remainder_types')]
    public function test_text_remainder_types_stay_strings(string $type, string $value): void
    {
        static::assertSame($value, $this->caster->cast($value, $type));
    }

    public function test_oid_is_cast_to_integer(): void
    {
        static::assertSame(42, $this->caster->cast('42', 'oid'));
    }

    public function test_timetz_keeps_the_clock_time_and_drops_the_offset(): void
    {
        static::assertSame('12:34:56', $this->caster->cast('12:34:56+02', 'timetz'));
        static::assertSame('12:34:56', $this->caster->cast('12:34:56-05:30', 'timetz'));
    }

    public function test_timestamp_is_marked_as_utc(): void
    {
        static::assertSame('2024-01-15 14:30:45+00:00', $this->caster->cast('2024-01-15 14:30:45', 'timestamp'));
    }

    public function test_timestamp_with_microseconds_is_marked_as_utc(): void
    {
        static::assertSame('2024-01-15 14:30:45.123456+00:00', $this->caster->cast(
            '2024-01-15 14:30:45.123456',
            'timestamp',
        ));
    }

    public function test_timestamp_negative_infinity_is_not_marked(): void
    {
        static::assertSame('-infinity', $this->caster->cast('-infinity', 'timestamp'));
    }

    public function test_timestamp_positive_infinity_is_not_marked(): void
    {
        static::assertSame('infinity', $this->caster->cast('infinity', 'timestamp'));
    }
}
