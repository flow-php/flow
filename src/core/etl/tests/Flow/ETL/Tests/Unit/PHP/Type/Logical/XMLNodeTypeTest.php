<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_int, type_xml_node};
use PHPUnit\Framework\TestCase;

final class XMLNodeTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_xml_node()->isEqual(type_xml_node())
        );
        self::assertFalse(
            type_xml_node()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_xml_node(true)->isValid(null));
        self::assertTrue(type_xml_node()->isValid(new \DOMDocument()));
        self::assertFalse(type_xml_node()->isValid('<xml></xml>'));
        self::assertFalse(type_xml_node()->isValid('2020-01-01'));
        self::assertFalse(type_xml_node()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml_node',
            type_xml_node()->toString()
        );
        self::assertSame(
            '?xml_node',
            type_xml_node(true)->toString()
        );
    }
}
