<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_float, type_integer, type_list, type_map, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class MapTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (type_map(type_string(), type_float()))->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            (type_map(type_string(), type_float()))->isEqual(type_list(type_integer()))
        );
        self::assertFalse(
            (type_map(type_string(), type_float()))->isEqual(type_map(type_string(), type_integer()))
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'map<string, string>',
            (type_map(type_string(), type_string()))->toString()
        );
    }

    public function test_using_nullable_map_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key cannot be nullable');

        type_map(type_string(true), type_string());
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (type_map(type_string(), type_string()))->isValid(['one' => 'two'])
        );
        self::assertTrue(
            (type_map(type_string(), type_string(), true))->isValid(null)
        );
        self::assertTrue(
            (type_map(type_integer(), type_list(type_integer())))->isValid([[1, 2], [3, 4]])
        );
        self::assertTrue(
            (type_map(type_integer(), type_map(type_string(), type_list(type_integer()))))
                ->isValid([0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]])
        );
        self::assertFalse(
            (type_map(type_integer(), type_string()))->isValid(['one' => 'two'])
        );
        self::assertFalse(
            (type_map(type_integer(), type_string()))->isValid([1, 2])
        );
        self::assertFalse(
            (type_map(type_string(), type_string()))->isValid(123)
        );
    }
}
