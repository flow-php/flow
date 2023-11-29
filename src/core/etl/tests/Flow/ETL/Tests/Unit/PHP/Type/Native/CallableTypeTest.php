<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\type_callable;
use function Flow\ETL\DSL\type_float;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class CallableTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_callable(false)->isEqual(type_callable(false))
        );
        $this->assertFalse(
            type_callable(false)->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            type_callable(false)->isEqual(type_float())
        );
        $this->assertFalse(
            type_callable(false)->isEqual(type_callable(true))
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'callable',
            type_callable(false)->toString()
        );
        $this->assertSame(
            '?callable',
            type_callable(true)->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            type_callable(false)->isValid('printf')
        );
        $this->assertFalse(
            type_callable(false)->isValid('one')
        );
        $this->assertFalse(
            type_callable(false)->isValid([1, 2])
        );
        $this->assertFalse(
            type_callable(false)->isValid(123)
        );
    }
}
