<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_float,
    type_from_array,
    type_integer,
    type_mixed,
    type_optional,
    type_string,
    type_union};
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class OptionalTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid null' => [
            'type' => type_optional(type_integer()),
            'value' => null,
            'exceptionClass' => null,
        ];

        yield 'valid integer' => [
            'type' => type_optional(type_integer()),
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'valid string for string type' => [
            'type' => type_optional(type_string()),
            'value' => 'string',
            'exceptionClass' => null,
        ];

        yield 'valid null for string type' => [
            'type' => type_optional(type_string()),
            'value' => null,
            'exceptionClass' => null,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'null stays as null for float type' => [
            'type' => type_optional(type_float()),
            'value' => null,
            'expected' => null,
            'exceptionClass' => null,
        ];

        yield 'float stays as is' => [
            'type' => type_optional(type_float()),
            'value' => 1.23445,
            'expected' => 1.23445,
            'exceptionClass' => null,
        ];

        yield 'string stays as is for string type' => [
            'type' => type_optional(type_string()),
            'value' => '1.23445',
            'expected' => '1.23445',
            'exceptionClass' => null,
        ];

        yield 'null stays as null for string type' => [
            'type' => type_optional(type_string()),
            'value' => null,
            'expected' => null,
            'exceptionClass' => null,
        ];
    }

    public static function invalid_creation_data_provider() : \Generator
    {
        yield 'optional type from another optional type' => [
            'type' => type_optional(type_float()),
            'exceptionMessage' => 'Optional type cannot be created from an optional type',
        ];

        yield 'optional type from mixed type' => [
            'type' => type_mixed(),
            'exceptionMessage' => 'Optional type cannot be created from MixedType, mixed is a standalone type',
        ];

        yield 'optional type from union type' => [
            'type' => type_union(type_float(), type_string()),
            'exceptionMessage' => 'Optional type cannot be created from a union type',
        ];

        yield 'optional type from union type with mixed' => [
            'type' => type_union(type_float(), type_integer()),
            'exceptionMessage' => 'Optional type cannot be created from a union type',
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid null' => [
            'type' => type_optional(type_integer()),
            'value' => null,
            'expected' => true,
        ];

        yield 'valid integer' => [
            'type' => type_optional(type_integer()),
            'value' => 1,
            'expected' => true,
        ];

        yield 'invalid string for integer type' => [
            'type' => type_optional(type_integer()),
            'value' => 'string',
            'expected' => false,
        ];

        yield 'valid string for string type' => [
            'type' => type_optional(type_string()),
            'value' => 'string',
            'expected' => true,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(OptionalType $type, mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->assert($value);
        } else {
            self::assertSame($value, $type->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(OptionalType $type, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->cast($value);
        } else {
            self::assertSame($expected, $type->cast($value));
        }
    }

    #[DataProvider('invalid_creation_data_provider')]
    public function test_invalid_creation(Type $type, string $exceptionMessage) : void
    {
        $this->expectExceptionMessage($exceptionMessage);
        type_optional($type);
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(OptionalType $type, mixed $value, bool $expected) : void
    {
        self::assertSame($expected, $type->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_optional(type_float());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            '?float',
            type_optional(type_float())->toString()
        );

        self::assertSame(
            '?string',
            type_optional(type_string())->toString()
        );
    }
}
