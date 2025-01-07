<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_boolean, type_float, type_integer, type_list, type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class ListTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (type_list(type_integer()))->isEqual(type_list(type_integer()))
        );
        self::assertFalse(
            (type_list(type_integer()))->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            (type_list(type_integer()))->isEqual(type_list(type_float()))
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'list<boolean>',
            (type_list(type_boolean()))->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (type_list(type_boolean()))->isValid([true, false])
        );
        self::assertTrue(
            (type_list(type_boolean(), true))->isValid(null)
        );
        self::assertTrue(
            (type_list(type_string()))->isValid(['one', 'two'])
        );
        self::assertTrue(
            (type_list(type_list(type_string())))->isValid([['one', 'two']])
        );
        self::assertTrue(
            (
                type_list(type_map(type_string(), type_list(type_integer())))
            )->isValid([['one' => [1, 2], 'two' => [3, 4]], ['one' => [5, 6], 'two' => [7, 8]]])
        );
        self::assertFalse(
            (type_list(type_string()))->isValid(['one' => 'two'])
        );
        self::assertFalse(
            (type_list(type_string()))->isValid([1, 2])
        );
        self::assertFalse(
            (type_list(type_string()))->isValid(123)
        );
    }
}
