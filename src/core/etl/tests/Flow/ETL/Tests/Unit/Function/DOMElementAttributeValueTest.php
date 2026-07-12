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
        static::assertEquals('foobar', ref('value')
            ->domElementAttributeValue('id')
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                flow_context(),
            ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_extracting_non_existing_attribute_from_dom_element_entry(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = HTMLDocument::createFromString('<span">foobar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(HTMLElement::class, $element->documentElement);
        static::assertNull(
            ref('value')
                ->domElementAttributeValue('id')
                ->eval(
                    row(flow_context(config())->entryFactory()->create('value', $element->documentElement)),
                    flow_context(),
                ),
        );
    }

    public function test_xml_extracting_attribute_from_dom_element_entry(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals('buz', ref('value')
            ->domElementAttributeValue('baz')
            ->eval(
                row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                flow_context(),
            ));
    }

    public function test_xml_extracting_non_existing_attribute_from_dom_element_entry(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertNull(
            ref('value')
                ->domElementAttributeValue('bar')
                ->eval(
                    row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)),
                    flow_context(),
                ),
        );
    }
}
