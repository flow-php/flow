<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\PostgreSql\ValueConverter\XMLConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class XMLConverterTest extends TestCase
{
    public function test_delegates_string_to_next_converter() : void
    {
        $converter = new XMLConverter();

        self::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_dom_document_returns_xml_string() : void
    {
        $converter = new XMLConverter();

        $doc = new \DOMDocument();
        $doc->loadXML('<root><item>test</item></root>');

        $result = $converter->toDatabase($doc);

        self::assertIsString($result);
        self::assertStringContainsString('<root><item>test</item></root>', $result);
    }

    public function test_dom_element_returns_xml_string() : void
    {
        $converter = new XMLConverter();

        $doc = new \DOMDocument();
        $doc->loadXML('<root><item id="test">Hello</item></root>');
        $element = $doc->getElementsByTagName('item')->item(0);

        self::assertInstanceOf(\DOMElement::class, $element);

        $result = $converter->toDatabase($element);

        self::assertIsString($result);
        self::assertStringContainsString('<item id="test">Hello</item>', $result);
    }

    public function test_null_returns_null() : void
    {
        $converter = new XMLConverter();

        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_xml() : void
    {
        $converter = new XMLConverter();

        self::assertSame([PostgreSqlType::XML], $converter->supportedTypes());
    }
}
