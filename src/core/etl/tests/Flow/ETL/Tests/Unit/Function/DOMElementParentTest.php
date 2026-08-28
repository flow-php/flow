<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

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
        ref('value')
            ->domElementParent()
            // @mago-ignore analysis:possibly-null-property-access
            ->eval(row($context->entryFactory()->create('value', $element->documentElement->parentElement)), $context);
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
            ref('value')
                ->domElementParent()
                ->eval(
                    row(flow_context(config())->entryFactory()->create('value', $element->querySelector('span'))),
                    flow_context(),
                ),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_parent_element_when_not_available(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span>bar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);
        static::assertNull(
            ref('value')
                ->domElementParent()
                ->eval(
                    row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                    flow_context(),
                ),
        );
    }

    public function test_xml_fails_when_parent_not_available_in_strict_mode(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        $context = flow_context(config());
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DOMElementParent requires non-null DOMNode or HTMLElement.');
        static::assertEquals($xml, ref('value')
            ->domElementParent()
            ->eval(row($context->entryFactory()->create('value', $xml->parentNode)), $context));
    }

    public function test_xml_getting_parent_element(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo>foo</foo><bar>bar</bar></root>');
        static::assertEquals(
            $xml->documentElement,
            ref('value')
                ->domElementParent()
                ->eval(
                    // @mago-ignore analysis:possibly-null-property-access
                    row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                    flow_context(),
                ),
        );
    }

    public function test_xml_getting_parent_element_when_not_available(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        static::assertEquals($xml, ref('value')
            ->domElementParent()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement)),
                flow_context(),
            ));
    }

    public function test_xml_getting_parent_element_when_passing_document(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root>foobar</root>');
        static::assertEquals($xml, ref('value')
            ->domElementParent()
            ->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context()));
    }
}
