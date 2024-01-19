<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_xml_node;
use PHPUnit\Framework\TestCase;

final class XMLNodeTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_xml_node()->isEqual(type_xml_node())
        );
        $this->assertFalse(
            type_xml_node()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        $this->assertTrue(type_xml_node()->isValid(new \DOMDocument()));
        $this->assertFalse(type_xml_node()->isValid('<xml></xml>'));
        $this->assertFalse(type_xml_node()->isValid('2020-01-01'));
        $this->assertFalse(type_xml_node()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'xml_node',
            type_xml_node()->toString()
        );
        $this->assertSame(
            '?xml_node',
            type_xml_node(true)->toString()
        );
    }
}
