<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, row};
use Dom\{HTMLDocument, HTMLElement};
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

final class DOMElementAttributeValueTest extends TestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_html_extracting_attribute_from_dom_element_entry() : void
    {
        $element = HTMLDocument::createFromString('<span id="foobar">foobar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertInstanceOf(HTMLElement::class, $element->documentElement);
        self::assertEquals(
            'foobar',
            ref('value')->domElementAttributeValue('id')->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context()
            )
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_extracting_non_existing_attribute_from_dom_element_entry() : void
    {
        $element = HTMLDocument::createFromString('<span">foobar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertInstanceOf(HTMLElement::class, $element->documentElement);
        self::assertNull(
            ref('value')->domElementAttributeValue('id')->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context()
            )
        );
    }

    public function test_xml_extracting_attribute_from_dom_element_entry() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            'buz',
            ref('value')->domElementAttributeValue('baz')->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context()
            )
        );
    }

    public function test_xml_extracting_non_existing_attribute_from_dom_element_entry() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertNull(
            ref('value')->domElementAttributeValue('bar')->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context()
            )
        );
    }
}
