<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_float, type_from_array, type_integer, type_optional, type_string, type_union};
use Flow\Types\Type\Logical\OptionalType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class OptionalTypeTest extends TestCase
{
    public static function optional_castable_data_provider() : \Generator
    {
        yield [type_optional(type_float()), null, null];
        yield [type_optional(type_float()), 1.23445, 1.23445];
        yield [type_optional(type_string()), '1.23445', '1.23445'];
        yield [type_optional(type_string()), null, null];
    }

    public function test_assert() : void
    {
        self::assertNull(type_optional(type_integer())->assert(null));
        self::assertSame(1, type_optional(type_integer())->assert(1));
    }

    /**
     * @param OptionalType<mixed> $type
     */
    #[DataProvider('optional_castable_data_provider')]
    public function test_casting_different_data_types_to_float(OptionalType $type, mixed $value, mixed $expected) : void
    {
        self::assertSame($expected, $type->cast($value));
    }

    public function test_creating_optional_type_from_another_optional_type() : void
    {
        $this->expectExceptionMessage('Optional type cannot be created from an optional type');
        type_optional(type_optional(type_float()));
    }

    public function test_creating_optional_type_from_union_type() : void
    {
        $this->expectExceptionMessage('Optional type cannot be created from a union type');
        type_optional(type_union(type_float(), type_string()));
    }

    public function test_normalizing_optional_type() : void
    {
        self::assertEquals(
            [
                'type' => 'optional',
                'base' => [
                    'type' => 'float',
                ],
            ],
            type_optional(type_float())->normalize()
        );

        self::assertEquals(
            type_optional(type_float()),
            type_from_array(type_optional(type_float())->normalize())
        );
    }
}
