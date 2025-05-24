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
    public static function casting_data_provider() : \Generator
    {
        yield [type_union(type_integer(), type_string()), '1', 1];
        yield [type_union(type_integer(), type_string()), 1, 1];
        yield [type_union(type_integer(), type_string()), 1.0, 1];
        yield [type_union(type_integer(), type_string()), false, 0];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield [type_union(type_integer(), type_string()), false];
        yield [type_union(type_integer(), type_string()), 1.0];
        yield [type_union(type_integer(), type_string()), null];
        yield [type_union(type_integer(), type_string()), new \stdClass()];
    }

    public static function valid_assert_data_provider() : \Generator
    {
        yield [type_union(type_integer(), type_string()), '1'];
        yield [type_union(type_integer(), type_string()), 1];
        yield [type_union(type_integer(), type_null()), 1];
        yield [type_union(type_integer(), type_null()), null];
        yield [type_union(type_integer(), type_string(), type_float()), '1.0'];
        yield [type_union(type_integer(), type_string(), type_float()), 1.0];
        yield [type_union(type_integer(), type_string(), type_float()), 1];
    }

    /**
     * @param UnionType<mixed, mixed> $type
     */
    #[DataProvider('casting_data_provider')]
    public function test_casting(UnionType $type, mixed $value, mixed $result) : void
    {
        self::assertEquals($result, $type->cast($value));
    }

    public function test_is_optional_type() : void
    {
        self::assertTrue(type_union(type_integer(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_null(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_integer(), type_null(), type_optional(type_string()))->isOptionalType());

    }

    public function test_normalization() : void
    {
        $type = type_union(type_integer(), type_string());

        self::assertEquals(
            [
                'type' => 'union',
                'left' => [
                    'type' => 'integer',
                ],
                'right' => [
                    'type' => 'string',
                ],
            ],
            $type->normalize()
        );

        self::assertEquals(
            $type,
            type_from_array($type->normalize())
        );
    }

    /**
     * @param UnionType<mixed, mixed> $type
     */
    #[DataProvider('valid_assert_data_provider')]
    public function test_successful_assert(UnionType $type, mixed $value) : void
    {
        self::assertTrue($type->isValid($value));
        self::assertSame($value, $type->assert($value));
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

    /**
     * @param UnionType<mixed, mixed> $type
     */
    #[DataProvider('invalid_assert_data_provider')]
    public function test_unsuccessful_assert(UnionType $type, mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);

        self::assertFalse($type->isValid($value));
        self::assertEquals($value, $type->assert($value));
    }
}
