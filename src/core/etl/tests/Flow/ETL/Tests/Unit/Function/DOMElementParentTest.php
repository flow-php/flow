<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class DOMElementParentTest extends TestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_html_fails_when_parent_not_available_in_strict_mode(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span>bar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);
        $context = flow_context(config());
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DOMElementParent requires non-null DOMNode or HTMLElement.');
        (new FunctionContext($context))->eval(
            ref('value')->domElementParent(),
            [
                // @mago-ignore analysis:possibly-null-property-access
                'value' => $element->documentElement->parentElement,
            ],
            schema(html_element_schema('value', nullable: true)),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_parent_element(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString(
            '<div><span>foo</span><p>bar</p></div>',
            LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR,
        );
        static::assertEquals(
            $element->documentElement,
            (new FunctionContext(flow_context()))->eval(
                lit($element->querySelector('span'))->domElementParent(),
                [],
                schema(),
            ),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_parent_element_when_not_available(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span>bar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);
        static::assertNull((new FunctionContext(flow_context()))->eval(
            lit($element->documentElement)->domElementParent(),
            [],
            schema(),
        ));
    }

    public function test_xml_fails_when_parent_not_available_in_strict_mode(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        $context = flow_context(config());
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DOMElementParent requires non-null DOMNode or HTMLElement.');
        static::assertEquals($xml, (new FunctionContext($context))->eval(
            ref('value')->domElementParent(),
            [
                'value' => $xml->parentNode,
            ],
            schema(xml_element_schema('value', nullable: true)),
        ));
    }

    public function test_xml_getting_parent_element(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo>foo</foo><bar>bar</bar></root>');
        static::assertEquals(
            $xml->documentElement,
            // a batch stores the element's markup, so the parent is only reachable on the live node
            // @mago-ignore analysis:possibly-null-property-access
            (new FunctionContext(flow_context()))->eval(
                lit($xml->documentElement->firstChild)->domElementParent(),
                [],
                schema(),
            ),
        );
    }

    public function test_xml_getting_parent_element_when_not_available(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->domElementParent(),
            [
                'value' => $xml->documentElement,
            ],
            schema(xml_element_schema('value')),
        ));
    }

    public function test_xml_getting_parent_element_when_passing_document(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->domElementParent(),
            ['value' => $xml],
            schema(xml_schema('value')),
        ));
    }
}
