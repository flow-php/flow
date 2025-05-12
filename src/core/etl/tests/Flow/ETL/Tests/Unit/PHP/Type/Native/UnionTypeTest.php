<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_boolean, type_float, type_from_array, type_int, type_null, type_optional, type_string, type_union, types};
use Flow\ETL\Exception\InvalidTypeException;
use Flow\ETL\PHP\Type\Native\UnionType;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class UnionTypeTest extends FlowTestCase
{
    public static function casting_data_provider() : \Generator
    {
        yield [type_union(type_int(), type_string()), '1', 1];
        yield [type_union(type_int(), type_string()), 1, 1];
        yield [type_union(type_int(), type_string()), 1.0, 1];
        yield [type_union(type_int(), type_string()), false, 0];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield [type_union(type_int(), type_string()), false];
        yield [type_union(type_int(), type_string()), 1.0];
        yield [type_union(type_int(), type_string()), null];
        yield [type_union(type_int(), type_string()), new \stdClass()];
    }

    public static function valid_assert_data_provider() : \Generator
    {
        yield [type_union(type_int(), type_string()), '1'];
        yield [type_union(type_int(), type_string()), 1];
        yield [type_union(type_int(), type_null()), 1];
        yield [type_union(type_int(), type_null()), null];
        yield [type_union(type_int(), type_string(), type_float()), '1.0'];
        yield [type_union(type_int(), type_string(), type_float()), 1.0];
        yield [type_union(type_int(), type_string(), type_float()), 1];
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
        self::assertTrue(type_union(type_int(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_null(), type_null())->isOptionalType());
        self::assertFalse(type_union(type_int(), type_null(), type_optional(type_string()))->isOptionalType());

    }

    public function test_normalization() : void
    {
        $type = type_union(type_int(), type_string());

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
            type_union(type_int(), type_string())->toString()
        );
        self::assertSame(
            'integer|null',
            type_union(type_int(), type_null())->toString()
        );
        self::assertSame(
            'integer|null|string',
            type_union(type_int(), type_string(), type_null())->toString()
        );
        self::assertSame(
            'integer|null|string',
            type_union(type_int(), type_string(), type_null(), type_optional(type_int()))->toString()
        );
        self::assertSame(
            'integer|string',
            type_union(type_int(), type_int(), type_int(), type_union(type_int(), type_string()))->toString()
        );
    }

    public function test_types() : void
    {
        self::assertEquals(
            types(type_int(), type_string()),
            type_union(type_int(), type_string())->types()
        );
        self::assertEquals(
            types(type_int(), type_string(), type_null()),
            type_union(type_int(), type_string(), type_null())->types()
        );
        self::assertEquals(
            types(type_int(), type_string(), type_optional(type_string())),
            type_union(type_int(), type_string(), type_optional(type_string()))->types()
        );
        self::assertEquals(
            types(type_int(), type_string(), type_float(), type_boolean()),
            type_union(type_int(), type_string(), type_union(type_float(), type_boolean()))->types()
        );
        self::assertEquals(
            types(type_int(), type_float(), type_boolean()),
            type_union(type_int(), type_int(), type_union(type_float(), type_boolean()))->types()->deduplicate()
        );
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
