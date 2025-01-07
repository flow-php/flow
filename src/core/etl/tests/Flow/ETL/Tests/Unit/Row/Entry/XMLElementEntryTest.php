<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\xml_element_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class XMLElementEntryTest extends FlowTestCase
{
    public function test_create_from_string() : void
    {
        $entry = xml_element_entry('node', '<node attr="test">value</node>');

        self::assertInstanceOf(\DOMElement::class, $entry->value());
        self::assertEquals('<node attr="test">value</node>', $entry->toString());
    }

    public function test_create_from_string_fails_with_invalid_xml() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given string "test" is not valid XML');

        xml_element_entry('node', 'test');
    }

    public function test_serialization() : void
    {
        $element = (new \DOMDocument())->createElement('testElement', 'This is a test');
        $element->setAttribute('test', 'value');

        $entry = xml_element_entry('node', clone $element);

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        self::assertTrue($entry->isEqual($unserialized));
        self::assertInstanceOf(\DOMElement::class, $entry->value());
        self::assertEquals($element->attributes, $entry->value()->attributes);
    }
}
