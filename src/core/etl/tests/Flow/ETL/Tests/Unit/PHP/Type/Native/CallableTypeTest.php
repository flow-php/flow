<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\CallableType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class CallableTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (new CallableType)->isEqual(new CallableType)
        );
        $this->assertFalse(
            (new CallableType)->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new CallableType)->isEqual(ScalarType::float())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'callable',
            (new CallableType)->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            (new CallableType)->isValid('printf')
        );
        $this->assertFalse(
            (new CallableType)->isValid('one')
        );
        $this->assertFalse(
            (new CallableType)->isValid([1, 2])
        );
        $this->assertFalse(
            (new CallableType)->isValid(123)
        );
    }
}
