<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class DOMElementValueTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_element_value_with_children(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<p><span>foobar</span></p>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertEquals('foobar', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementValue(),
            [
                'value' => $element->documentElement,
            ],
            schema(html_element_schema('value')),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_simple_element_value(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span>bar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertEquals('bar', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementValue(),
            [
                'value' => $element->documentElement,
            ],
            schema(html_element_schema('value')),
        ));
    }

    public function test_xml_getting_element_value_with_children(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo><bar>baz</bar></foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals('baz', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementValue(),
            [
                'value' => $xml->documentElement->firstChild,
            ],
            schema(xml_element_schema('value')),
        ));
    }

    public function test_xml_getting_simple_element_value(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals('bar', (new FunctionContext(flow_context()))->eval(
            ref('value')->domElementValue(),
            [
                'value' => $xml->documentElement->firstChild,
            ],
            schema(xml_element_schema('value')),
        ));
    }
}
