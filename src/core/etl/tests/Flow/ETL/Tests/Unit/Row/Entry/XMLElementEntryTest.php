<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\xml_element_entry;
use function Flow\Types\DSL\type_instance_of;

final class XMLElementEntryTest extends FlowTestCase
{
    public function test_create_from_dom_document(): void
    {
        $document = new \DOMDocument();
        $document->loadXML('<root><name>User Name</name><id>01</id></root>');

        /* @phpstan-ignore-next-line */
        $entry = xml_element_entry('node', $document->documentElement->firstChild);

        static::assertInstanceOf(\DOMElement::class, $entry->value());
        static::assertSame('<name>User Name</name>', $entry->toString());
        static::assertSame($document->documentElement, $entry->value()->parentNode);
    }

    public function test_create_from_string(): void
    {
        $entry = xml_element_entry('node', '<node attr="test">value</node>');

        static::assertInstanceOf(\DOMElement::class, $entry->value());
        static::assertSame('<node attr="test">value</node>', $entry->toString());
    }

    public function test_create_from_string_fails_with_invalid_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given string "test" is not valid XML');

        xml_element_entry('node', 'test');
    }

    public function test_duplicating_entry(): void
    {
        $entry = xml_element_entry('node', '<node attr="test">value</node>');
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertSame($entry->toString(), $duplicated->toString());
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = xml_element_entry('old_name', '<node attr="test">value</node>', $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals($entry->toString(), $renamedEntry->toString());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_serialization(): void
    {
        $element = (new \DOMDocument())->createElement('testElement', 'This is a test');
        $element->setAttribute('test', 'value');

        $entry = xml_element_entry('node', clone $element);

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        static::assertTrue($entry->isEqual(type_instance_of(XMLElementEntry::class)->assert($unserialized)));
        static::assertInstanceOf(\DOMElement::class, $entry->value());
        static::assertEquals($element->attributes, $entry->value()->attributes);
    }
}
