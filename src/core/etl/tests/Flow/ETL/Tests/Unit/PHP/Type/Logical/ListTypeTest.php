<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\{ListType, MapType};
use PHPUnit\Framework\TestCase;

final class ListTypeTest extends TestCase
{
    public function test_element() : void
    {
        self::assertEquals(
            $element = ListElement::integer(),
            (new ListType($element))->element()
        );
    }

    public function test_equals() : void
    {
        self::assertTrue(
            (new ListType(ListElement::integer()))->isEqual(new ListType(ListElement::integer()))
        );
        self::assertFalse(
            (new ListType(ListElement::integer()))->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        self::assertFalse(
            (new ListType(ListElement::integer()))->isEqual(new ListType(ListElement::float()))
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'list<boolean>',
            (new ListType(ListElement::boolean()))->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (new ListType(ListElement::boolean()))->isValid([true, false])
        );
        self::assertTrue(
            (new ListType(ListElement::string()))->isValid(['one', 'two'])
        );
        self::assertTrue(
            (new ListType(ListElement::list(new ListType(ListElement::string()))))->isValid([['one', 'two']])
        );
        self::assertTrue(
            (
                new ListType(
                    ListElement::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                )
            )->isValid([['one' => [1, 2], 'two' => [3, 4]], ['one' => [5, 6], 'two' => [7, 8]]])
        );
        self::assertFalse(
            (new ListType(ListElement::string()))->isValid(['one' => 'two'])
        );
        self::assertFalse(
            (new ListType(ListElement::string()))->isValid([1, 2])
        );
        self::assertFalse(
            (new ListType(ListElement::string()))->isValid(123)
        );
    }

    public function test_value() : void
    {
        self::assertEquals(
            $value = MapValue::string(),
            (new MapType(MapKey::string(), $value))->value()
        );
    }
}
