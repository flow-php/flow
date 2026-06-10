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
        yield 'timetz' => ['timetz'];
        yield 'interval' => ['interval'];
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
