<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class DOMElementValueTest extends TestCase
{
    public function test_getting_element_value_with_children() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo><bar>baz</bar></foo></root>');

        self::assertEquals(
            'baz',
            ref('value')->domElementValue()->eval(Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }

    public function test_getting_simple_element_value() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        self::assertEquals(
            'bar',
            ref('value')->domElementValue()->eval(Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild)))
        );
    }
}
