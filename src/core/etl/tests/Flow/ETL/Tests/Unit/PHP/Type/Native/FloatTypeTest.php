<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_callable, type_float, type_int, type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class FloatTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_float(false)->isEqual(type_float(false))
        );
        self::assertFalse(
            type_float(false)->isEqual(type_map(type_string(), type_int()))
        );
        self::assertFalse(
            type_float(false)->isEqual(type_callable(false))
        );
        self::assertFalse(
            type_float(false)->isEqual(type_float(true))
        );
    }

    public function test_from_array() : void
    {
        self::assertTrue(
            type_float(false)->isEqual(type_float(false)->fromArray(['type' => 'float', 'nullable' => false, 'precision' => 6]))
        );
        self::assertTrue(
            type_float(true, 12)->isEqual(type_float(true)->fromArray(['type' => 'float', 'nullable' => true, 'precision' => 12]))
        );
    }

    public function test_is_comparable_with() : void
    {
        self::assertTrue(
            type_float(false)->isComparableWith(type_float(false))
        );
        self::assertTrue(
            type_float(false)->isComparableWith(type_float(true))
        );
        self::assertTrue(
            type_float(true)->isComparableWith(type_float(false))
        );
        self::assertTrue(
            type_float(true)->isComparableWith(type_float(true))
        );
        self::assertTrue(
            type_float(false)->isComparableWith(type_int())
        );
        self::assertFalse(
            type_float(false)->isComparableWith(type_string())
        );
        self::assertFalse(
            type_float(false)->isComparableWith(type_callable(false))
        );
        self::assertFalse(
            type_float(false)->isComparableWith(type_map(type_string(), type_int()))
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            type_float(false)->isEqual(type_float(false))
        );
        self::assertFalse(
            type_float(false)->isEqual(type_float(true))
        );
        self::assertFalse(
            type_float(false)->isEqual(type_int())
        );
    }

    public function test_merge() : void
    {
        self::assertTrue(
            type_float(false)->merge(type_float(false))->isEqual(type_float(false))
        );
        self::assertTrue(
            type_float(false)->merge(type_float(true))->isEqual(type_float(true))
        );
        self::assertTrue(
            type_float(true)->merge(type_float(false))->isEqual(type_float(true))
        );
        self::assertTrue(
            type_float(true)->merge(type_float(true))->isEqual(type_float(true))
        );
        self::assertTrue(
            type_float(true, 6)->merge(type_float(true, 3))->isEqual(type_float(true, 3))
        );

        $this->expectExceptionMessage('Cannot merge different types, float and string');
        type_float(false)->merge(type_string());
    }

    public function test_normalize() : void
    {
        self::assertSame(
            [
                'type' => 'float',
                'nullable' => false,
                'precision' => 6,
            ],
            type_float(false)->normalize()
        );
        self::assertSame(
            [
                'type' => 'float',
                'nullable' => true,
                'precision' => 6,
            ],
            type_float(true)->normalize()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'float',
            type_float(false)->toString()
        );
        self::assertSame(
            '?float',
            type_float(true)->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_float(false)->isValid(1.0)
        );
        self::assertTrue(
            type_float(true)->isValid(null)
        );
        self::assertFalse(
            type_float(false)->isValid('one')
        );
        self::assertFalse(
            type_float(false)->isValid([1, 2])
        );
        self::assertFalse(
            type_float(false)->isValid(123)
        );
    }
}
