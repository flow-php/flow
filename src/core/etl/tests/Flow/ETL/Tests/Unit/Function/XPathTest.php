<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class XPathTest extends FlowTestCase
{
    public function test_xpath_on_simple_xml_with_only_one_node_returned() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            [$xml->documentElement->firstChild],
            ref('value')->xpath('/root/foo')->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context())
        );
    }

    public function test_xpath_when_there_are_more_than_one_elements_under_given_path() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo><foo baz="buz">bar</foo></root>');

        self::assertInstanceOf(\DOMElement::class, $xml->documentElement);
        self::assertEquals(
            [
                $xml->documentElement->firstChild,
                $xml->documentElement->lastChild,
            ],
            ref('value')->xpath('/root/foo')->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context())
        );
    }

    public function test_xpath_with_invalid_path_syntax() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertNull(
            ref('value')->xpath('/root/foo/asa')->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context())
        );
    }

    public function test_xpath_with_non_existing_path() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertNull(
            ref('value')->xpath('/root/bar')->eval(row(flow_context(config())->entryFactory()->create('value', $xml)), flow_context())
        );
    }
}
