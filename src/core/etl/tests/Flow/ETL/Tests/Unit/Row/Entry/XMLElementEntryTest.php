<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\XMLElementEntry;
use PHPUnit\Framework\TestCase;

final class XMLElementEntryTest extends TestCase
{
    public function test_create_from_string() : void
    {
        $entry = new XMLElementEntry(
            'node',
            '<node attr="test">value</node>'
        );

        self::assertInstanceOf(\DOMElement::class, $entry->value());
        self::assertEquals('<node attr="test">value</node>', $entry->toString());
    }

    public function test_create_from_string_fails_with_invalid_xml() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given string "test" is not valid XML');

        new XMLElementEntry(
            'node',
            'test'
        );
    }

    public function test_serialization() : void
    {
        $element = (new \DOMDocument())->createElement('testElement', 'This is a test');
        $element->setAttribute('test', 'value');

        $entry = new XMLElementEntry(
            'node',
            clone $element,
        );

        $serialized = \serialize($entry);
        $unserialized = \unserialize($serialized);

        self::assertTrue($entry->isEqual($unserialized));
        self::assertInstanceOf(\DOMElement::class, $entry->value());
        self::assertEquals($element->attributes, $entry->value()->attributes);
    }
}
