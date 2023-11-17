<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class DOMNodeValueTest extends TestCase
{
    public function test_getting_node_value_with_children() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo><bar>baz</bar></foo></root>');

        $this->assertEquals(
            'baz',
            ref('value')->domNodeValue()->eval(Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }

    public function test_getting_simple_node_value() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        $this->assertEquals(
            'bar',
            ref('value')->domNodeValue()->eval(Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }
}
