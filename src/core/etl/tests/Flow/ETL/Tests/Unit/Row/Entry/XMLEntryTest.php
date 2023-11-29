<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\XMLEntry;
use PHPUnit\Framework\TestCase;

final class XMLEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        $doc1 = new \DOMDocument();
        $doc1->loadXML('<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        $doc2 = new \DOMDocument();
        $doc2->loadXML('<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');

        yield 'equal names and equal simple xml documents' => [
            true,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];

        $doc1 = new \DOMDocument();
        $doc1->loadXML('<root><foo foo="bar" bar="foo">1</foo><bar>2</bar><baz>3</baz></root>');
        $doc2 = new \DOMDocument();
        $doc2->loadXML('<root><foo bar="foo" foo="bar">1</foo><bar>2</bar><baz>3</baz></root>');

        yield 'equal names and equal simple xml documents with different order of attributes' => [
            true,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];

        $doc1 = new \DOMDocument();
        $doc1->loadXML('<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        $doc2 = new \DOMDocument();
        $doc2->loadXML('<root><foo bar="foo" foo="bar">1</foo><bar>2</bar><baz>3</baz></root>');

        yield 'equal nodes but different attributes' => [
            false,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];

        $doc1 = new \DOMDocument();
        $doc1->loadXML('<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        $doc2 = new \DOMDocument();
        $doc2->loadXML('<root><bar>2</bar><baz>3</baz></root>');

        yield 'equal attributes but different nodes' => [
            false,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];

        $doc1 = new \DOMDocument();
        $doc1->loadXML('<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        $doc2 = new \DOMDocument();

        yield 'compare with empty document' => [
            false,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];

        $doc1 = new \DOMDocument();
        $doc2 = new \DOMDocument();

        yield 'compare twp empty documents' => [
            true,
            new XMLEntry('name', $doc1),
            new XMLEntry('name', $doc2),
        ];
    }

    /**
     * The C14N() method in PHP's DOMDocument class does not provide an option to remove all whitespace between nodes;
     * it's designed to produce a canonical form of the XML document according to the Canonical XML standard,
     * which generally preserves whitespace within text nodes.
     */
    public function test_canonicalization() : void
    {
        $doc = new \DOMDocument();
        $doc->loadXML('<item item_attribute_01="1"><id id_attribute_01="1">1</id></item>');

        $doc2 = new \DOMDocument();
        $doc2->loadXML(<<<'XML'
<item item_attribute_01="1">
            <id id_attribute_01="1">1</id>
        </item>
XML);

        $this->assertNotEquals(
            xml_entry('row', $doc),
            xml_entry('row', $doc2),
        );
    }

    public function test_creating_entry_from_invalid_xml_string() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given string "foo" is not valid XML');

        new XMLEntry('name', 'foo');
    }

    public function test_creating_entry_from_valid_xml_string() : void
    {
        $entry = new XMLEntry('name', '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');

        $this->assertSame('name', $entry->name());
        $this->assertSame("<?xml version=\"1.0\"?>\n<root><foo>1</foo><bar>2</bar><baz>3</baz></root>\n", $entry->__toString());
    }

    public function test_creating_xml_entry_with_empty_dom_document() : void
    {
        $doc = new \DOMDocument();
        $entry = new XMLEntry('name', $doc);

        $this->assertSame('name', $entry->name());
        $this->assertSame($doc, $entry->value());
        $this->assertSame("<?xml version=\"1.0\"?>\n", $entry->__toString());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, XMLEntry $entry, XMLEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }
}
