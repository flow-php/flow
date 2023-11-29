<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_float;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use PHPUnit\Framework\TestCase;

final class ArrayTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (type_array())->isEqual(new ArrayType)
        );
        $this->assertTrue(
            ArrayType::empty()->isEqual(ArrayType::empty())
        );
        $this->assertFalse(
            (type_array())->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (type_array())->isEqual(type_float())
        );
        $this->assertFalse(
            ArrayType::empty()->isEqual(type_array())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'array<mixed>',
            type_array()->toString()
        );
        $this->assertSame(
            'array<empty, empty>',
            ArrayType::empty()->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            type_array()->isValid([])
        );
        $this->assertTrue(
            type_array()->isValid(['one'])
        );
        $this->assertTrue(
            type_array()->isValid([1])
        );
        $this->assertFalse(
            type_array()->isValid(null)
        );
        $this->assertFalse(
            type_array()->isValid('one')
        );
        $this->assertFalse(
            type_array()->isValid(true)
        );
        $this->assertFalse(
            type_array()->isValid(123)
        );
    }
}
