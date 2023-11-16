<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class XPathTest extends TestCase
{
    public function test_xpath_on_simple_xml_with_only_one_node_returned() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        $this->assertEquals(
            $xml->documentElement->firstChild,
            ref('value')->xpath('/root/foo')->eval(Row::create((new NativeEntryFactory())->create('value', $xml)))
        );
    }

    public function test_xpath_when_there_are_more_than_one_elements_under_given_path() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo><foo baz="buz">bar</foo></root>');

        $this->assertEquals(
            [
                $xml->documentElement->firstChild,
                $xml->documentElement->lastChild,
            ],
            ref('value')->xpath('/root/foo')->eval(Row::create((new NativeEntryFactory())->create('value', $xml)))
        );
    }

    public function test_xpath_with_invalid_path_syntax() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        $this->assertNull(
            ref('value')->xpath('/root/foo/@')->eval(Row::create((new NativeEntryFactory())->create('value', $xml)))
        );
    }

    public function test_xpath_with_non_existing_path() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        $this->assertNull(
            ref('value')->xpath('/root/bar')->eval(Row::create((new NativeEntryFactory())->create('value', $xml)))
        );
    }
}
