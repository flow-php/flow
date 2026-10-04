<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use Flow\ETL\Tests\Context\FunctionContext;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class DOMElementAttributeValueTest extends TestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_html_extracting_attribute_from_dom_element_entry(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString(
            '<span id="foobar">foobar</span>',
            LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR,
        );

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertEquals('foobar', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementAttributeValue('id'),
            [
                'value' => $element->documentElement,
            ],
            schema(html_element_schema('value')),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_extracting_non_existing_attribute_from_dom_element_entry(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span">foobar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->domElementAttributeValue('id'),
            [
                'value' => $element->documentElement,
            ],
            schema(html_element_schema('value')),
        ));
    }

    public function test_xml_extracting_attribute_from_dom_element_entry(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals('buz', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementAttributeValue('baz'),
            [
                'value' => $xml->documentElement->firstChild,
            ],
            schema(xml_element_schema('value')),
        ));
    }

    public function test_xml_extracting_non_existing_attribute_from_dom_element_entry(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->domElementAttributeValue('bar'),
            [
                'value' => $xml->documentElement->firstChild,
            ],
            schema(xml_element_schema('value')),
        ));
    }
}
