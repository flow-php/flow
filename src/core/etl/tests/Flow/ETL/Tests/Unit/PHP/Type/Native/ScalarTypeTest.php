<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\TestCase;

final class ScalarTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            ScalarType::integer()->isEqual(ScalarType::integer())
        );
        $this->assertFalse(
            ScalarType::integer()->isEqual(new MapType(MapKey::string(), MapValue::float()))
        );
        $this->assertFalse(
            ScalarType::integer()->isEqual(ScalarType::float())
        );
    }

    public function test_optional() : void
    {
        $this->assertFalse(
            ScalarType::string()->optional()
        );
        $this->assertTrue(
            ScalarType::boolean(true)->optional()
        );
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'boolean',
            ScalarType::boolean()->toString()
        );
        $this->assertSame(
            '?string',
            ScalarType::string(true)->toString()
        );
    }

    public function test_valid() : void
    {
        $this->assertTrue(
            ScalarType::boolean()->isValid(true)
        );
        $this->assertTrue(
            ScalarType::string()->isValid('one')
        );
        $this->assertTrue(
            ScalarType::integer()->isValid(1)
        );
        $this->assertTrue(
            ScalarType::integer(true)->isValid(null)
        );
        $this->assertFalse(
            ScalarType::integer()->isValid('one')
        );
        $this->assertFalse(
            ScalarType::string()->isValid([1, 2])
        );
        $this->assertFalse(
            ScalarType::boolean()->isValid(123)
        );
    }
}
