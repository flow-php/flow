<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DOMDocument;
use DOMElement;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class XPathTest extends FlowTestCase
{
    public function test_xpath_on_simple_xml_with_only_one_node_returned(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals(
            [$xml->documentElement->firstChild],
            ref('value')->xpath('/root/foo')->eval(row(['value' => $xml]), flow_context()),
        );
    }

    public function test_xpath_when_there_are_more_than_one_elements_under_given_path(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals(
            [
                $xml->documentElement->firstChild,
                $xml->documentElement->lastChild,
            ],
            ref('value')->xpath('/root/foo')->eval(row(['value' => $xml]), flow_context()),
        );
    }

    public function test_xpath_with_invalid_path_syntax(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull(ref('value')->xpath('/root/foo/asa')->eval(row(['value' => $xml]), flow_context()));
    }

    public function test_xpath_with_non_existing_path(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull(ref('value')->xpath('/root/bar')->eval(row(['value' => $xml]), flow_context()));
    }

    public function test_xpath_selecting_non_element_nodes_returns_null(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull(ref('value')->xpath('/root/foo/text()')->eval(row(['value' => $xml]), flow_context()));
        static::assertNull(ref('value')->xpath('/root/foo/@baz')->eval(row(['value' => $xml]), flow_context()));
    }
}
