<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class ListTypeTest extends TestCase
{
    public function test_element() : void
    {
        $this->assertEquals(
            $element = ListElement::integer(),
            (new ListType($element))->element()
        );
    }

    public function test_equals() : void
    {
        $this->assertTrue(
            (new ListType(ListElement::integer()))->isEqual(new ListType(ListElement::integer()))
        );
        $this->assertFalse(
            (new ListType(ListElement::integer()))->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new ListType(ListElement::integer()))->isEqual(new ListType(ListElement::float()))
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'list<boolean>',
            (new ListType(ListElement::boolean()))->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new ListType(ListElement::boolean()))->isValid([true, false])
        );
        $this->assertTrue(
            (new ListType(ListElement::string()))->isValid(['one', 'two'])
        );
        $this->assertTrue(
            (new ListType(ListElement::list(new ListType(ListElement::string()))))->isValid([['one', 'two']])
        );
        $this->assertTrue(
            (
                new ListType(
                    ListElement::map(new MapType(MapKey::string(), MapValue::list(new ListType(ListElement::integer()))))
                )
            )->isValid([['one' => [1, 2], 'two' => [3, 4]], ['one' => [5, 6], 'two' => [7, 8]]])
        );
        $this->assertFalse(
            (new ListType(ListElement::string()))->isValid(['one' => 'two'])
        );
        $this->assertFalse(
            (new ListType(ListElement::string()))->isValid([1, 2])
        );
        $this->assertFalse(
            (new ListType(ListElement::string()))->isValid(123)
        );
    }

    public function test_value() : void
    {
        $this->assertEquals(
            $value = MapValue::string(),
            (new MapType(MapKey::string(), $value))->value()
        );
    }
}
