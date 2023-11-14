<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class ArrayTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (new ArrayType())->isEqual(new ArrayType)
        );
        $this->assertTrue(
            ArrayType::empty()->isEqual(ArrayType::empty())
        );
        $this->assertFalse(
            (new ArrayType())->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new ArrayType())->isEqual(ScalarType::float())
        );
        $this->assertFalse(
            ArrayType::empty()->isEqual(new ArrayType)
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'array<mixed>',
            (new ArrayType())->toString()
        );
        $this->assertSame(
            'array<empty, empty>',
            ArrayType::empty()->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new ArrayType())->isValid([])
        );
        $this->assertTrue(
            (new ArrayType())->isValid(['one'])
        );
        $this->assertTrue(
            (new ArrayType())->isValid([1])
        );
        $this->assertFalse(
            (new ArrayType())->isValid(null)
        );
        $this->assertFalse(
            (new ArrayType())->isValid('one')
        );
        $this->assertFalse(
            (new ArrayType())->isValid(true)
        );
        $this->assertFalse(
            (new ArrayType())->isValid(123)
        );
    }
}
