<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\XMLEntry;
use PHPUnit\Framework\TestCase;

final class XMLEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        XMLEntry::fromString('', '<xml><name>node</name></xml>');
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (XMLEntry::fromString('0', '<xml><name>node</name></xml>'))->name());
    }

    public function test_returns_json_as_value() : void
    {
        $xml = '<xml><name>node</name></xml>';
        $entry = XMLEntry::fromString('item', $xml);

        $this->assertStringContainsString($xml, $entry->value()->saveXML());
    }

    public function test_map() : void
    {
        $xml = '<xml><name>node</name></xml>';
        $entry = XMLEntry::fromString('item', $xml)->map(function (\DOMDocument $value) : \DOMDocument {
            $dom = new \DOMDocument();
            $dom->loadXML("<html>{$value->firstChild->firstChild->nodeValue}</html>");

            return $dom;
        });

        $this->assertStringContainsString(
            '<html>node</html>',
            $entry->value()->saveXML()
        );
    }

    public function test_renames_entry() : void
    {
        $xml = '<xml><name>node</name></xml>';
        $entry = XMLEntry::fromString('item', $xml);
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(string $document, string $otherDocument, bool $diff) : void
    {
        $this->assertSame($diff, XMLEntry::fromString('entry', $document)->isEqual(XMLEntry::fromString('entry', $otherDocument)));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'similar xml documents' => [
            '<xml>node</xml>',
            '<xml>node</xml>',
            true,
        ];
        yield 'similar xml documents with nested elements and attributes' => [
            '<xml><body><h1 data-attr="test">node</h1></body></xml>',
            '<xml><body><h1 data-attr="test">node</h1></body></xml>',
            true,
        ];
        yield 'similar xml documents with attributes' => [
            '<xml data-something="test">node</xml>',
            '<xml data-something="test">node</xml>',
            true,
        ];
        yield 'similar xml documents with different attributes' => [
            '<xml data-something="test">node</xml>',
            '<xml data-something="nothing">node</xml>',
            false,
        ];
        yield 'similar xml documents with different node values' => [
            '<xml>node</xml>',
            '<xml>root</xml>',
            false,
        ];
        yield 'different xml documents' => [
            '<xml>node</xml>',
            '<html>root</html>',
            false,
        ];
    }
}
