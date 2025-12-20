<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types;

use Flow\PostgreSql\Client\Types\ResultCaster;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ResultCasterTest extends TestCase
{
    private ResultCaster $caster;

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_float_types() : \Generator
    {
        yield 'float4' => ['float4'];
        yield 'float8' => ['float8'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_small_integer_types() : \Generator
    {
        yield 'int2' => ['int2'];
        yield 'int4' => ['int4'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_string_types() : \Generator
    {
        yield 'text' => ['text'];
        yield 'varchar' => ['varchar'];
        yield 'json' => ['json'];
        yield 'jsonb' => ['jsonb'];
        yield 'uuid' => ['uuid'];
        yield 'date' => ['date'];
        yield 'timestamp' => ['timestamp'];
        yield 'timestamptz' => ['timestamptz'];
        yield 'time' => ['time'];
        yield 'timetz' => ['timetz'];
        yield 'interval' => ['interval'];
        yield 'numeric' => ['numeric'];
        yield 'money' => ['money'];
        yield 'inet' => ['inet'];
        yield 'cidr' => ['cidr'];
    }

    protected function setUp() : void
    {
        $this->caster = new ResultCaster();
    }

    public function test_bool_false() : void
    {
        self::assertFalse($this->caster->cast('f', 'bool'));
    }

    public function test_bool_true() : void
    {
        self::assertTrue($this->caster->cast('t', 'bool'));
    }

    public function test_bytea_decodes_hex() : void
    {
        self::assertSame('hello', $this->caster->cast('\x68656c6c6f', 'bytea'));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_infinity(string $type) : void
    {
        self::assertSame(\INF, $this->caster->cast('Infinity', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_nan(string $type) : void
    {
        self::assertNan($this->caster->cast('NaN', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_negative_infinity(string $type) : void
    {
        self::assertSame(-\INF, $this->caster->cast('-Infinity', $type));
    }

    #[DataProvider('provide_float_types')]
    public function test_float_types(string $type) : void
    {
        self::assertSame(3.14, $this->caster->cast('3.14', $type));
        self::assertSame(-0.5, $this->caster->cast('-0.5', $type));
        self::assertSame(0.0, $this->caster->cast('0', $type));
    }

    public function test_int8_on_64bit_platform() : void
    {
        if (\PHP_INT_SIZE < 8) {
            self::markTestSkipped('Test requires 64-bit PHP');
        }

        self::assertSame(9223372036854775807, $this->caster->cast('9223372036854775807', 'int8'));
        self::assertSame(42, $this->caster->cast('42', 'int8'));
    }

    public function test_null_type_returns_string() : void
    {
        self::assertSame('some value', $this->caster->cast('some value', null));
    }

    #[DataProvider('provide_small_integer_types')]
    public function test_small_integer_types(string $type) : void
    {
        self::assertSame(42, $this->caster->cast('42', $type));
        self::assertSame(-100, $this->caster->cast('-100', $type));
        self::assertSame(0, $this->caster->cast('0', $type));
    }

    #[DataProvider('provide_string_types')]
    public function test_string_types_pass_through(string $type) : void
    {
        $value = 'test string value';
        self::assertSame($value, $this->caster->cast($value, $type));
    }
}
