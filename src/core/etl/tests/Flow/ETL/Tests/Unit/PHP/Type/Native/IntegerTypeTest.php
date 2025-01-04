<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_callable, type_float, type_int, type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class IntegerTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_int(false)->isEqual(type_int(false))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_map(type_string(), type_int()))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_callable(false))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_int(true))
        );
    }

    public function test_from_array() : void
    {
        self::assertTrue(
            type_int(false)->isEqual(type_int(false)->fromArray(['type' => 'integer', 'nullable' => false]))
        );
        self::assertTrue(
            type_int(true)->isEqual(type_int(true)->fromArray(['type' => 'integer', 'nullable' => true]))
        );
    }

    public function test_is_comparable_with() : void
    {
        self::assertTrue(
            type_int(false)->isComparableWith(type_int(false))
        );
        self::assertTrue(
            type_int(false)->isComparableWith(type_int(true))
        );
        self::assertTrue(
            type_int(true)->isComparableWith(type_int(false))
        );
        self::assertTrue(
            type_int(true)->isComparableWith(type_int(true))
        );
        self::assertTrue(
            type_int(false)->isComparableWith(type_float(false))
        );
        self::assertFalse(
            type_int(false)->isComparableWith(type_string())
        );
        self::assertFalse(
            type_int(false)->isComparableWith(type_callable(false))
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            type_int(false)->isEqual(type_int(false))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_map(type_string(), type_int()))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_callable(false))
        );
        self::assertFalse(
            type_int(false)->isEqual(type_int(true))
        );
    }

    public function test_merge() : void
    {
        self::assertTrue(
            type_int(false)->isEqual(type_int(false)->merge(type_int(false)))
        );
        self::assertTrue(
            type_int(true)->isEqual(type_int(true)->merge(type_int(true)))
        );
        self::assertTrue(
            type_int(true)->isEqual(type_int(false)->merge(type_int(true)))
        );
        self::assertTrue(
            type_int(true)->isEqual(type_int(true)->merge(type_int(false)))
        );
        self::assertEquals(
            type_float(true, 12),
            type_int(true)->merge(type_float(true, 12))
        );
    }

    public function test_normalize() : void
    {
        self::assertSame(
            ['type' => 'integer', 'nullable' => false],
            type_int(false)->normalize()
        );
        self::assertSame(
            ['type' => 'integer', 'nullable' => true],
            type_int(true)->normalize()
        );
    }

    public function test_nullable() : void
    {
        self::assertFalse(
            type_int(false)->nullable()
        );
        self::assertTrue(
            type_int(true)->nullable()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'integer',
            type_int(false)->toString()
        );
        self::assertSame(
            '?integer',
            type_int(true)->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_int(false)->isValid(1)
        );
        self::assertTrue(
            type_int(true)->isValid(null)
        );
        self::assertFalse(
            type_int(false)->isValid('one')
        );
        self::assertFalse(
            type_int(false)->isValid([1, 2])
        );
        self::assertFalse(
            type_int(false)->isValid(123.0)
        );
    }
}
