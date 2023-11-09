<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ResourceType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class ResourceTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            (new ResourceType)->isEqual(new ResourceType)
        );
        $this->assertFalse(
            (new ResourceType)->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            (new ResourceType)->isEqual(ScalarType::float())
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'resource',
            (new ResourceType)->toString()
        );
    }

    public function test_valid() : void
    {
        $handle = \fopen('php://temp/max', 'r+b');
        $this->assertTrue(
            (new ResourceType)->isValid($handle)
        );
        \fclose($handle);
        $this->assertFalse(
            (new ResourceType)->isValid('one')
        );
        $this->assertFalse(
            (new ResourceType)->isValid([1, 2])
        );
        $this->assertFalse(
            (new ResourceType)->isValid(123)
        );
    }
}
