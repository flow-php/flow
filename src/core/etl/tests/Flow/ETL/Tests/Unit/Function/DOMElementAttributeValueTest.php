<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, row};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Tests\FlowTestCase;

final class DOMElementAttributeValueTest extends FlowTestCase
{
    public function test_extracting_attribute_from_dom_element_entry() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertEquals(
            'buz',
            ref('value')->domElementAttributeValue('baz')->eval(
                row((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild))
            )
        );
    }

    public function test_extracting_non_existing_attribute_from_dom_element_entry() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        self::assertNull(
            ref('value')->domElementAttributeValue('bar')->eval(
                row((new NativeEntryFactory())->create('value', $xml->documentElement->firstChild))
            )
        );
    }
}
