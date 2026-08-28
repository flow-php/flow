<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class OptionalTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
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

    public static function cast_data_provider(): Generator
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

    public static function invalid_creation_data_provider(): Generator
    {
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

    public static function is_valid_data_provider(): Generator
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(OptionalType $type, mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->assert($value);
        } else {
            static::assertSame($value, $type->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(OptionalType $type, mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->cast($value);
        } else {
            static::assertSame($expected, $type->cast($value));
        }
    }

    #[DataProvider('invalid_creation_data_provider')]
    public function test_invalid_creation(Type $type, string $exceptionMessage): void
    {
        $this->expectExceptionMessage($exceptionMessage);
        type_optional($type);
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(OptionalType $type, mixed $value, bool $expected): void
    {
        static::assertSame($expected, $type->isValid($value));
    }

    public function test_double_wrapping_collapses(): void
    {
        static::assertSame('?string', type_optional(type_optional(type_string()))->toString());
        static::assertTrue(type_equals(type_optional(type_optional(type_string())), type_optional(type_string())));
    }

    public function test_collapsing_is_idempotent_at_any_depth(): void
    {
        static::assertSame(
            '?list<?integer>',
            type_optional(type_optional(type_optional(type_list(type_optional(type_integer())))))->toString(),
        );
    }

    public function test_mixed_and_union_are_still_refused(): void
    {
        try {
            type_optional(type_mixed());
            static::fail('MixedType must be refused');
        } catch (InvalidTypeException $e) {
            static::assertSame(
                'Optional type cannot be created from MixedType, mixed is a standalone type',
                $e->getMessage(),
            );
        }

        try {
            type_optional(type_union(type_float(), type_string()));
            static::fail('UnionType must be refused');
        } catch (InvalidTypeException $e) {
            static::assertSame('Optional type cannot be created from a union type', $e->getMessage());
        }
    }

    public function test_normalization(): void
    {
        $type = type_optional(type_float());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('?float', type_optional(type_float())->toString());

        static::assertSame('?string', type_optional(type_string())->toString());
    }
}
