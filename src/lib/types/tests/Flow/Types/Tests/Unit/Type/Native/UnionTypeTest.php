<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Native\UnionType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\types;

final class UnionTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
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
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
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

    public static function is_valid_data_provider(): Generator
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(UnionType $type, mixed $value, ?string $exceptionClass = null): void
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
    public function test_cast(UnionType $type, mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->cast($value);
        } else {
            static::assertEquals($expected, $type->cast($value));
        }
    }

    public function test_is_optional_type(): void
    {
        static::assertTrue((new UnionType(type_integer(), type_null()))->isOptionalType());
        static::assertFalse((new UnionType(type_null(), type_null()))->isOptionalType());
        static::assertFalse(
            (new UnionType(new UnionType(type_integer(), type_null()), type_optional(type_string())))->isOptionalType(),
        );
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(UnionType $type, mixed $value, bool $expected): void
    {
        static::assertSame($expected, $type->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_union(type_integer(), type_string());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('integer|string', type_union(type_integer(), type_string())->toString());
        static::assertSame('integer|null', type_union(type_integer(), type_null())->toString());
        static::assertSame('integer|null|string', type_union(type_integer(), type_string(), type_null())->toString());
        static::assertSame(
            'integer|null|string',
            type_union(type_integer(), type_string(), type_null(), type_optional(type_integer()))->toString(),
        );
        static::assertSame(
            'integer|string',
            type_union(
                type_integer(),
                type_integer(),
                type_integer(),
                type_union(type_integer(), type_string()),
            )->toString(),
        );
    }

    public function test_types(): void
    {
        static::assertEquals(
            types(type_integer(), type_string()),
            (new UnionType(type_integer(), type_string()))->types(),
        );
        static::assertEquals(
            types(type_integer(), type_string(), type_null()),
            (new UnionType(new UnionType(type_integer(), type_string()), type_null()))->types(),
        );
        static::assertEquals(
            types(type_integer(), type_string(), type_optional(type_string())),
            (new UnionType(new UnionType(type_integer(), type_string()), type_optional(type_string())))->types(),
        );
        static::assertEquals(
            types(type_integer(), type_string(), type_float(), type_boolean()),
            (new UnionType(
                new UnionType(type_integer(), type_string()),
                new UnionType(type_float(), type_boolean()),
            ))->types(),
        );
        static::assertEquals(
            types(type_integer(), type_float(), type_boolean()),
            (new UnionType(new UnionType(type_integer(), type_integer()), new UnionType(type_float(), type_boolean())))
                ->types()
                ->deduplicate(),
        );
    }

    public function test_union_member_for_picks_first_valid(): void
    {
        $type = type_union(type_string(), type_integer());

        static::assertEquals(type_string(), $type->memberFor('5'));
        static::assertEquals(type_integer(), $type->memberFor(5));
    }

    public function test_union_member_for_returns_null_when_no_member_matches(): void
    {
        static::assertNull(type_union(type_string(), type_integer())->memberFor(new stdClass()));
    }

    public function test_union_member_for_skips_the_null_member(): void
    {
        static::assertNull(type_union(type_null(), type_string())->memberFor(null));
    }

    public function test_union_member_for_unwraps_optional_members(): void
    {
        static::assertEquals(type_integer(), type_union(type_optional(type_integer()), type_string())->memberFor(5));
    }

    public function test_union_with_mixed_type(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('UnionType cannot be mixed with MixedType, mixed is a standalone type');

        type_union(type_integer(), type_mixed());
    }
}
