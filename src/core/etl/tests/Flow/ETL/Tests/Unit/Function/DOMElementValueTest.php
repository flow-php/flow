<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, row};
use Dom\{HTMLDocument, HTMLElement};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

final class DOMElementValueTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_html_getting_element_value_with_children() : void
    {
        $element = HTMLDocument::createFromString('<p><span>foobar</span></p>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertInstanceOf(HTMLElement::class, $element->documentElement);
        self::assertEquals(
            'foobar',
            ref('value')->domElementValue()->eval(row(flow_context(config())->entryFactory()->create('value', $element->documentElement)))
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_getting_simple_element_value() : void
    {
        $element = HTMLDocument::createFromString('<span>bar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertInstanceOf(HTMLElement::class, $element->documentElement);
        self::assertEquals(
            'bar',
            ref('value')->domElementValue()->eval(row(flow_context(config())->entryFactory()->create('value', $element->documentElement)))
        );
    }

    public function test_xml_getting_element_value_with_children() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo><bar>baz</bar></foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            'baz',
            ref('value')->domElementValue()->eval(row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)))
        );
    }

    public function test_xml_getting_simple_element_value() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            'bar',
            ref('value')->domElementValue()->eval(row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)))
        );
    }
}
