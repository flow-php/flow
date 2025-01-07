<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_array, type_float};
use function Flow\ETL\DSL\{type_map, type_string};
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (type_array())->isEqual(type_array())
        );
        self::assertTrue(
            ArrayType::empty()->isEqual(ArrayType::empty())
        );
        self::assertFalse(
            (type_array())->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            (type_array())->isEqual(type_float())
        );
        self::assertFalse(
            ArrayType::empty()->isEqual(type_array())
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'array<mixed>',
            type_array()->toString()
        );
        self::assertSame(
            'array<empty, empty>',
            ArrayType::empty()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_array()->isValid([])
        );
        self::assertTrue(
            type_array()->isValid(['one'])
        );
        self::assertTrue(
            type_array()->isValid([1])
        );
        self::assertTrue(
            type_array(nullable: true)->isValid(null)
        );
        self::assertFalse(
            type_array()->isValid(null)
        );
        self::assertFalse(
            type_array()->isValid('one')
        );
        self::assertFalse(
            type_array()->isValid(true)
        );
        self::assertFalse(
            type_array()->isValid(123)
        );
    }
}
