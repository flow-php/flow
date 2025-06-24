<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_literal};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class LiteralTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid string literal' => [
            'literal' => 'hello',
            'value' => 'hello',
            'exceptionClass' => null,
        ];

        yield 'valid integer literal' => [
            'literal' => 42,
            'value' => 42,
            'exceptionClass' => null,
        ];

        yield 'valid float literal' => [
            'literal' => 3.14,
            'value' => 3.14,
            'exceptionClass' => null,
        ];

        yield 'valid boolean true literal' => [
            'literal' => true,
            'value' => true,
            'exceptionClass' => null,
        ];

        yield 'valid boolean false literal' => [
            'literal' => false,
            'value' => false,
            'exceptionClass' => null,
        ];

        yield 'invalid string literal' => [
            'literal' => 'hello',
            'value' => 'world',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer literal' => [
            'literal' => 42,
            'value' => 24,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float literal' => [
            'literal' => 3.14,
            'value' => 2.71,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean literal' => [
            'literal' => true,
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid type mismatch' => [
            'literal' => 'hello',
            'value' => 42,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null value' => [
            'literal' => 'hello',
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array value' => [
            'literal' => 'hello',
            'value' => ['hello'],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object value' => [
            'literal' => 'hello',
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'valid string literal' => [
            'literal' => 'hello',
            'value' => 'hello',
            'expected' => 'hello',
            'exceptionClass' => null,
        ];

        yield 'valid integer literal' => [
            'literal' => 42,
            'value' => 42,
            'expected' => 42,
            'exceptionClass' => null,
        ];

        yield 'valid float literal' => [
            'literal' => 3.14,
            'value' => 3.14,
            'expected' => 3.14,
            'exceptionClass' => null,
        ];

        yield 'valid boolean literal' => [
            'literal' => true,
            'value' => true,
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'invalid string literal' => [
            'literal' => 'hello',
            'value' => 'world',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid integer literal' => [
            'literal' => 42,
            'value' => 24,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid type mismatch' => [
            'literal' => 'hello',
            'value' => 42,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid null value' => [
            'literal' => 'hello',
            'value' => null,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid string literal' => [
            'literal' => 'hello',
            'value' => 'hello',
            'expected' => true,
        ];

        yield 'valid integer literal' => [
            'literal' => 42,
            'value' => 42,
            'expected' => true,
        ];

        yield 'valid float literal' => [
            'literal' => 3.14,
            'value' => 3.14,
            'expected' => true,
        ];

        yield 'valid boolean true literal' => [
            'literal' => true,
            'value' => true,
            'expected' => true,
        ];

        yield 'valid boolean false literal' => [
            'literal' => false,
            'value' => false,
            'expected' => true,
        ];

        yield 'invalid string literal' => [
            'literal' => 'hello',
            'value' => 'world',
            'expected' => false,
        ];

        yield 'invalid integer literal' => [
            'literal' => 42,
            'value' => 24,
            'expected' => false,
        ];

        yield 'invalid float literal' => [
            'literal' => 3.14,
            'value' => 2.71,
            'expected' => false,
        ];

        yield 'invalid boolean literal' => [
            'literal' => true,
            'value' => false,
            'expected' => false,
        ];

        yield 'invalid type mismatch string to int' => [
            'literal' => 'hello',
            'value' => 42,
            'expected' => false,
        ];

        yield 'invalid type mismatch int to string' => [
            'literal' => 42,
            'value' => '42',
            'expected' => false,
        ];

        yield 'invalid type mismatch float to string' => [
            'literal' => 3.14,
            'value' => '3.14',
            'expected' => false,
        ];

        yield 'invalid null value' => [
            'literal' => 'hello',
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid array value' => [
            'literal' => 'hello',
            'value' => ['hello'],
            'expected' => false,
        ];

        yield 'invalid object value' => [
            'literal' => 'hello',
            'value' => new \stdClass(),
            'expected' => false,
        ];
    }

    public static function to_string_data_provider() : \Generator
    {
        yield 'string literal' => [
            'literal' => 'hello',
            'expected' => "'hello'",
        ];

        yield 'integer literal' => [
            'literal' => 42,
            'expected' => '42',
        ];

        yield 'float literal' => [
            'literal' => 3.14,
            'expected' => '3.14',
        ];

        yield 'boolean true literal' => [
            'literal' => true,
            'expected' => 'true',
        ];

        yield 'boolean false literal' => [
            'literal' => false,
            'expected' => 'false',
        ];

        yield 'empty string literal' => [
            'literal' => '',
            'expected' => "''",
        ];

        yield 'zero integer literal' => [
            'literal' => 0,
            'expected' => '0',
        ];

        yield 'zero float literal' => [
            'literal' => 0.0,
            'expected' => '0',
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(bool|float|int|string $literal, mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_literal($literal)->assert($value);
        } else {
            self::assertSame($literal, type_literal($literal)->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(bool|float|int|string $literal, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_literal($literal)->cast($value);
        } else {
            self::assertSame($expected, type_literal($literal)->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(bool|float|int|string $literal, mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_literal($literal)->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_literal('hello');
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_normalization_with_boolean() : void
    {
        $type = type_literal(true);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_normalization_with_float() : void
    {
        $type = type_literal(3.14);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_normalization_with_integer() : void
    {
        $type = type_literal(42);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    #[DataProvider('to_string_data_provider')]
    public function test_to_string(bool|float|int|string $literal, string $expected) : void
    {
        self::assertSame($expected, type_literal($literal)->toString());
    }
}
