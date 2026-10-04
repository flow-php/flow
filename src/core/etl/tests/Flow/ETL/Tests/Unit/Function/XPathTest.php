<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DOMDocument;
use DOMElement;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;

final class XPathTest extends FlowTestCase
{
    public function test_xpath_on_simple_xml_with_only_one_node_returned(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertEquals(
            [$xml->documentElement->firstChild],
            (new FunctionContext(flow_context()))->eval(
                ref('value')->xpath('/root/foo'),
                ['value' => $xml],
                schema(xml_schema('value')),
            ),
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
            (new FunctionContext(flow_context()))->eval(
                ref('value')->xpath('/root/foo'),
                ['value' => $xml],
                schema(xml_schema('value')),
            ),
        );
    }

    public function test_xpath_with_invalid_path_syntax(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->xpath('/root/foo/asa'),
            ['value' => $xml],
            schema(xml_schema('value')),
        ));
    }

    public function test_xpath_with_non_existing_path(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->xpath('/root/bar'),
            ['value' => $xml],
            schema(xml_schema('value')),
        ));
    }

    public function test_xpath_selecting_non_element_nodes_returns_null(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->xpath('/root/foo/text()'),
            ['value' => $xml],
            schema(xml_schema('value')),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->xpath('/root/foo/@baz'),
            ['value' => $xml],
            schema(xml_schema('value')),
        ));
    }
}
