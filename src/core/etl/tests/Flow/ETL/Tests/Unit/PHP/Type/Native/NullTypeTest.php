<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_null;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class NullTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_null()->isEqual(type_null())
        );
        $this->assertFalse(
            type_null()->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            type_null()->isEqual(type_float())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'null',
            type_null()->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            type_null()->isValid(null)
        );
        $this->assertFalse(
            type_null()->isValid('one')
        );
        $this->assertFalse(
            type_null()->isValid([1, 2])
        );
        $this->assertFalse(
            type_null()->isValid(123)
        );
    }
}
