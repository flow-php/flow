<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_resource;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use PHPUnit\Framework\TestCase;

final class ResourceTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (type_resource(false))->isEqual(type_resource(false))
        );
        $this->assertFalse(
            (type_resource(false))->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (type_resource(false))->isEqual(type_float())
        );
        $this->assertFalse(
            (type_resource(false))->isEqual(type_resource())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'resource',
            (type_resource(false))->toString()
        );
        $this->assertSame(
            '?resource',
            (type_resource())->toString()
        );
    }

    public function test_valid() : void
    {
        $handle = \fopen('php://temp/max', 'r+b');
        $this->assertTrue(
            (type_resource(false))->isValid($handle)
        );
        \fclose($handle);
        $this->assertFalse(
            (type_resource(false))->isValid('one')
        );
        $this->assertFalse(
            (type_resource(false))->isValid([1, 2])
        );
        $this->assertFalse(
            (type_resource(false))->isValid(123)
        );
    }
}
