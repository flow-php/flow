<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_xml;
use PHPUnit\Framework\TestCase;

final class XMLTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_xml()->isEqual(type_xml())
        );
        $this->assertFalse(
            type_xml()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        $this->assertTrue(type_xml()->isValid(new \DOMDocument()));
        $this->assertFalse(type_xml()->isValid('<xml></xml>'));
        $this->assertFalse(type_xml()->isValid('2020-01-01'));
        $this->assertFalse(type_xml()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'xml',
            type_xml()->toString()
        );
        $this->assertSame(
            '?xml',
            type_xml(true)->toString()
        );
    }
}
