<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class DOMElementAttributesCountTest extends TestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_html_attributes_count_on_element_with_multiple_attributes(): void
    {
        $element = HTMLDocument::createFromString(
            '<span data-attr="1" data-foo="2" data-bar="3">foobar</span>',
            LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR,
        );

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertSame(3, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context(),
            ));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_attributes_count_on_element_with_one_attribute(): void
    {
        $element = HTMLDocument::createFromString(
            '<span data-attr="1">foobar</span>',
            LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR,
        );

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertSame(1, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context(),
            ));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_attributes_count_on_element_with_zero_attributes(): void
    {
        $element = HTMLDocument::createFromString('<span>foobar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertSame(0, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context(),
            ));
    }

    public function test_xml_attributes_count_on_element_with_multiple_attributes(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo atr-01="1" atr-02="2" atr-03="3">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertSame(3, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context(),
            ));
    }

    public function test_xml_attributes_count_on_element_with_one_attribute(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertSame(1, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context(),
            ));
    }

    public function test_xml_attributes_count_on_element_with_zero_attributes(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertSame(0, ref('value')
            ->domElementAttributesCount()
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context(),
            ));
    }
}
