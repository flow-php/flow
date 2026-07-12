<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Dom\Element;
use Dom\XmlDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\xml_element_entry;
use function Flow\Types\DSL\type_instance_of;
use function serialize;
use function unserialize;

final class XMLElementEntryTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_create_from_dom_xmldocument(): void
    {
        // @mago-ignore analysis:unavailable-method
        $document = XmlDocument::createFromString('<root><name>User Name</name><id>01</id></root>');
        static::assertNotNull($document->documentElement);

        $firstChild = $document->documentElement->firstChild;
        static::assertInstanceOf(Element::class, $firstChild);

        $entry = xml_element_entry('node', $firstChild);
        $value = $entry->value();
        static::assertInstanceOf(Element::class, $value);
        static::assertSame('<name>User Name</name>', $entry->toString());
        static::assertSame($document->documentElement, $value->parentNode);
    }

    public function test_create_from_dom_document(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><name>User Name</name><id>01</id></root>');
        static::assertNotNull($document->documentElement);
        $firstChild = $document->documentElement->firstChild;
        static::assertInstanceOf(DOMElement::class, $firstChild);

        $entry = xml_element_entry('node', $firstChild);
        $value = $entry->value();
        static::assertInstanceOf(DOMElement::class, $value);
        static::assertSame('<name>User Name</name>', $entry->toString());
        static::assertSame($document->documentElement, $value->parentNode);
    }

    public function test_create_from_string(): void
    {
        $entry = xml_element_entry('node', '<node attr="test">value</node>');
        static::assertInstanceOf(DOMElement::class, $entry->value());
        static::assertSame('<node attr="test">value</node>', $entry->toString());
    }

    public function test_create_from_string_fails_with_invalid_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given string "test" is not valid XML');
        xml_element_entry('node', 'test');
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
        $element = type_instance_of(DOMElement::class)->assert((new DOMDocument())->createElement(
            'testElement',
            'This is a test',
        ));
        $element->setAttribute('test', 'value');
        $entry = xml_element_entry('node', clone $element);
        $serialized = serialize($entry);
        $unserialized = type_instance_of(XMLElementEntry::class)->assert(unserialize($serialized));
        static::assertTrue($entry->isEqual($unserialized));
        static::assertInstanceOf(DOMElement::class, $entry->value());
        static::assertEquals($element->attributes, $entry->value()->attributes);
    }
}
