<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class NullTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (new NullType)->isEqual(new NullType)
        );
        $this->assertFalse(
            (new NullType)->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new NullType)->isEqual(ScalarType::float())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'null',
            (new NullType)->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new NullType)->isValid(null)
        );
        $this->assertFalse(
            (new NullType)->isValid('one')
        );
        $this->assertFalse(
            (new NullType)->isValid([1, 2])
        );
        $this->assertFalse(
            (new NullType)->isValid(123)
        );
    }
}
