<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_int, type_xml_element};
use PHPUnit\Framework\TestCase;

final class XMLElementTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_xml_element()->isEqual(type_xml_element())
        );
        self::assertFalse(
            type_xml_element()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_xml_element(true)->isValid(null));
        self::assertTrue(type_xml_element()->isValid(new \DOMElement('xml')));
        self::assertFalse(type_xml_element()->isValid('<xml></xml>'));
        self::assertFalse(type_xml_element()->isValid('2020-01-01'));
        self::assertFalse(type_xml_element()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml_element',
            type_xml_element()->toString()
        );
        self::assertSame(
            '?xml_element',
            type_xml_element(true)->toString()
        );
    }
}
