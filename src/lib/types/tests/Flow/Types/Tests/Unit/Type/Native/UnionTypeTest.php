<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_boolean,
    type_float,
    type_from_array,
    type_integer,
    type_mixed,
    type_null,
    type_optional,
    type_string,
    type_union,
    types};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Native\UnionType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class UnionTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid string' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => '1',
            'exceptionClass' => null,
        ];

        yield 'valid integer' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'valid integer with null union' => [
            'type' => type_union(type_integer(), type_null()),
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'valid null with null union' => [
            'type' => type_union(type_integer(), type_null()),
            'value' => null,
            'exceptionClass' => null,
        ];

        yield 'valid string with multiple types' => [
            'type' => type_union(type_integer(), type_string(), type_float()),
            'value' => '1.0',
            'exceptionClass' => null,
        ];

        yield 'valid float with multiple types' => [
            'type' => type_union(type_integer(), type_string(), type_float()),
            'value' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'valid integer with multiple types' => [
            'type' => type_union(type_integer(), type_string(), type_float()),
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'invalid boolean' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1.0,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string to integer' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => '1',
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'integer stays as is' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'float to integer' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1.0,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'boolean to integer' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => false,
            'expected' => 0,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid string' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => '1',
            'expected' => true,
        ];

        yield 'valid integer' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1,
            'expected' => true,
        ];

        yield 'valid null with null union' => [
            'type' => type_union(type_integer(), type_null()),
            'value' => null,
            'expected' => true,
        ];

        yield 'invalid boolean' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => false,
            'expected' => false,
        ];

        yield 'invalid float' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => 1.0,
            'expected' => false,
        ];

        yield 'invalid null' => [
            'type' => type_union(type_integer(), type_string()),
            'value' => null,
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(UnionType $type, mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->assert($value);
        } else {
            self::assertSame($value, $type->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(UnionType $type, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->cast($value);
        } else {
            self::assertEquals($expected, $type->cast($value));
        }
    }

    public function test_is_optional_type() : void
    {
        self::assertTrue(type_union(type_integer(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_null(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_integer(), type_null(), type_optional(type_string()))->isOptionalType());
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(UnionType $type, mixed $value, bool $expected) : void
    {
        self::assertSame($expected, $type->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_union(type_integer(), type_string());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'integer|string',
            type_union(type_integer(), type_string())->toString()
        );
        self::assertSame(
            'integer|null',
            type_union(type_integer(), type_null())->toString()
        );
        self::assertSame(
            'integer|null|string',
            type_union(type_integer(), type_string(), type_null())->toString()
        );
        self::assertSame(
            'integer|null|string',
            type_union(type_integer(), type_string(), type_null(), type_optional(type_integer()))->toString()
        );
        self::assertSame(
            'integer|string',
            type_union(type_integer(), type_integer(), type_integer(), type_union(type_integer(), type_string()))->toString()
        );
    }

    public function test_types() : void
    {
        self::assertEquals(
            types(type_integer(), type_string()),
            type_union(type_integer(), type_string())->types()
        );
        self::assertEquals(
            types(type_integer(), type_string(), type_null()),
            type_union(type_integer(), type_string(), type_null())->types()
        );
        self::assertEquals(
            types(type_integer(), type_string(), type_optional(type_string())),
            type_union(type_integer(), type_string(), type_optional(type_string()))->types()
        );
        self::assertEquals(
            types(type_integer(), type_string(), type_float(), type_boolean()),
            type_union(type_integer(), type_string(), type_union(type_float(), type_boolean()))->types()
        );
        self::assertEquals(
            types(type_integer(), type_float(), type_boolean()),
            type_union(type_integer(), type_integer(), type_union(type_float(), type_boolean()))->types()->deduplicate()
        );
    }

    public function test_union_with_mixed_type() : void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('UnionType cannot be mixed with MixedType, mixed is a standalone type');

        type_union(type_integer(), type_mixed());
    }
}
