<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, row};
use Dom\HTMLDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

final class DOMElementParentTest extends TestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_html_fails_when_parent_not_available_in_strict_mode() : void
    {
        $element = HTMLDocument::createFromString('<span>bar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DOMElementParent requires non-null DOMNode or HTMLElement.');

        ref('value')->domElementParent()->eval(row($context->entryFactory()->create('value', $element->documentElement->parentElement)), $context);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_getting_parent_element() : void
    {
        $element = HTMLDocument::createFromString('<div><span>foo</span><p>bar</p></div>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertEquals(
            $element->documentElement,
            ref('value')->domElementParent()->eval(row(flow_context(config())->entryFactory()->create('value', $element->querySelector('span'))), flow_context())
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_getting_parent_element_when_not_available() : void
    {
        $element = HTMLDocument::createFromString('<span>bar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertNull(
            ref('value')->domElementParent()->eval(row(flow_context(config())->entryFactory()->create('value', $element->documentElement)), flow_context())
        );
    }

    public function test_xml_fails_when_parent_not_available_in_strict_mode() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root>foobar</root>');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DOMElementParent requires non-null DOMNode or HTMLElement.');

        self::assertEquals(
            $xml,
            ref('value')->domElementParent()->eval(row($context->entryFactory()->create('value', $xml->parentNode)), $context)
        );
    }

    public function test_xml_getting_parent_element() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>foo</foo><bar>bar</bar></root>');

        self::assertEquals(
            $xml->documentElement,
            /* @phpstan-ignore-next-line */
            ref('value')->domElementParent()->eval(row(flow_context(config())->entryFactory()->create('value', $xml->documentElement->firstChild)), flow_context())
        );
    }

    public function test_xml_getting_parent_element_when_not_available() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root>foobar</root>');

        self::assertEquals(
            $xml,
            ref('value')->domElementParent()->eval(row(flow_context(config())->entryFactory()->create('value', $xml->documentElement)), flow_context())
        );
    }

    public function test_xml_getting_parent_element_when_passing_document() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root>foobar</root>');

        self::assertEquals(
            $xml,
            ref('value')->domElementParent()->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context())
        );
    }
}
