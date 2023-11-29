<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class ScalarTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_int()->isEqual(type_int())
        );
        $this->assertFalse(
            type_int()->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            type_int()->isEqual(type_float())
        );
    }

    public function test_nullable() : void
    {
        $this->assertFalse(
            type_string(false)->nullable()
        );
        $this->assertTrue(
            type_boolean(true)->nullable()
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'boolean',
            type_boolean()->toString()
        );
        $this->assertSame(
            '?string',
            type_string(true)->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            type_boolean()->isValid(true)
        );
        $this->assertTrue(
            type_string()->isValid('one')
        );
        $this->assertTrue(
            type_int()->isValid(1)
        );
        $this->assertTrue(
            type_int(true)->isValid(null)
        );
        $this->assertFalse(
            type_int()->isValid('one')
        );
        $this->assertFalse(
            type_string()->isValid([1, 2])
        );
        $this->assertFalse(
            type_boolean()->isValid(123)
        );
    }
}
