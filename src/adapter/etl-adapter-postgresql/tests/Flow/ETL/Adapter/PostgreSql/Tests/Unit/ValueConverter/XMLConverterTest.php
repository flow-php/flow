<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use DOMDocument;
use DOMElement;
use Flow\ETL\Adapter\PostgreSql\ValueConverter\XMLConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class XMLConverterTest extends TestCase
{
    public function test_delegates_string_to_next_converter(): void
    {
        $converter = new XMLConverter();

        static::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_dom_document_returns_xml_string(): void
    {
        $converter = new XMLConverter();

        $doc = new DOMDocument();
        $doc->loadXML('<root><item>test</item></root>');

        $result = $converter->toDatabase($doc);

        static::assertIsString($result);
        static::assertStringContainsString('<root><item>test</item></root>', $result);
    }

    public function test_dom_element_returns_xml_string(): void
    {
        $converter = new XMLConverter();

        $doc = new DOMDocument();
        $doc->loadXML('<root><item id="test">Hello</item></root>');
        $element = $doc->getElementsByTagName('item')->item(0);

        static::assertInstanceOf(DOMElement::class, $element);

        $result = $converter->toDatabase($element);

        static::assertIsString($result);
        static::assertStringContainsString('<item id="test">Hello</item>', $result);
    }

    public function test_null_returns_null(): void
    {
        $converter = new XMLConverter();

        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_xml(): void
    {
        $converter = new XMLConverter();

        static::assertSame([ValueType::XML], $converter->supportedTypes());
    }
}
