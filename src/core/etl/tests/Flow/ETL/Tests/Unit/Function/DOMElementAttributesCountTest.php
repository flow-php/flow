<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Tests\FlowTestCase;

final class DOMElementAttributesCountTest extends FlowTestCase
{
    public function test_attributes_count_on_element_with_multiple_attributes() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo atr-01="1" atr-02="2" atr-03="3">bar</foo></root>');

        self::assertEquals(
            3,
            ref('value')->domElementAttributesCount()->eval(
                Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild))
            )
        );
    }

    public function test_attributes_count_on_element_with_one_attribute() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertEquals(
            1,
            ref('value')->domElementAttributesCount()->eval(
                Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild))
            )
        );
    }

    public function test_attributes_count_on_element_with_zero_attributes() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo>bar</foo></root>');

        self::assertEquals(
            0,
            ref('value')->domElementAttributesCount()->eval(
                Row::create((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild))
            )
        );
    }
}
