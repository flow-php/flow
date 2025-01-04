<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_boolean, type_callable, type_int, type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class BooleanTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_boolean(false)->isEqual(type_boolean(false))
        );
        self::assertFalse(
            type_boolean(false)->isEqual(type_map(type_string(), type_int()))
        );
        self::assertFalse(
            type_boolean(false)->isEqual(type_callable(false))
        );
        self::assertFalse(
            type_boolean(false)->isEqual(type_boolean(true))
        );
    }

    public function test_from_array() : void
    {
        self::assertTrue(
            type_boolean(false)->isEqual(type_boolean(false)->fromArray(['type' => 'boolean', 'nullable' => false]))
        );
        self::assertTrue(
            type_boolean(true)->isEqual(type_boolean(true)->fromArray(['type' => 'boolean', 'nullable' => true]))
        );
    }

    public function test_is_comparable_with() : void
    {
        self::assertTrue(
            type_boolean(false)->isComparableWith(type_boolean(false))
        );
        self::assertTrue(
            type_boolean(false)->isComparableWith(type_boolean(true))
        );
        self::assertTrue(
            type_boolean(true)->isComparableWith(type_boolean(false))
        );
        self::assertTrue(
            type_boolean(true)->isComparableWith(type_boolean(true))
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            type_boolean(false)->isEqual(type_boolean(false))
        );
        self::assertFalse(
            type_boolean(false)->isEqual(type_boolean(true))
        );
    }

    public function test_make_nullable() : void
    {
        self::assertTrue(
            type_boolean(false)->makeNullable(true)->isEqual(type_boolean(true))
        );
        self::assertTrue(
            type_boolean(true)->makeNullable(false)->isEqual(type_boolean(false))
        );
    }

    public function test_merge() : void
    {
        self::assertTrue(
            type_boolean(false)->merge(type_boolean(false))->isEqual(type_boolean(false))
        );
        self::assertTrue(
            type_boolean(false)->merge(type_boolean(true))->isEqual(type_boolean(true))
        );
        self::assertTrue(
            type_boolean(true)->merge(type_boolean(false))->isEqual(type_boolean(true))
        );
        self::assertTrue(
            type_boolean(true)->merge(type_boolean(true))->isEqual(type_boolean(true))
        );
    }

    public function test_normalize() : void
    {
        self::assertSame(
            ['type' => 'boolean', 'nullable' => false],
            type_boolean(false)->normalize()
        );
        self::assertSame(
            ['type' => 'boolean', 'nullable' => true],
            type_boolean(true)->normalize()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'boolean',
            type_boolean(false)->toString()
        );
        self::assertSame(
            '?boolean',
            type_boolean(true)->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_boolean(false)->isValid(true)
        );
        self::assertTrue(
            type_boolean(true)->isValid(null)
        );
        self::assertFalse(
            type_boolean(false)->isValid('one')
        );
        self::assertFalse(
            type_boolean(false)->isValid([1, 2])
        );
        self::assertFalse(
            type_boolean(false)->isValid(123)
        );
    }
}
