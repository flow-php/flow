<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\XML;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Test\FilesystemTestHelper;
use PHPUnit\Framework\TestCase;

final class XMLReaderExtractorTest extends TestCase
{
    use FilesystemTestHelper;

    public function test_limit() : void
    {
        $path = $this->createTemporaryFile('xml_extractor_signal_stop', '.csv');

        \file_put_contents($path, <<<'XML'
<?xml version="1.0"?>
<items items_attribute_01="1" items_attribute_02="2">
        <item item_attribute_01="1">
            <id id_attribute_01="1">1</id>
        </item>
        <item item_attribute_01="2">
            <id id_attribute_01="2">2</id>
        </item>
        <item item_attribute_01="3">
            <id id_attribute_01="3">3</id>
        </item>
        <item item_attribute_01="4">
            <id id_attribute_01="4">4</id>
        </item>
        <item item_attribute_01="5">
            <id id_attribute_01="5">5</id>
        </item>
    </items>

XML);

        $extractor = new XMLReaderExtractor(Path::realpath($path), 'items/item');
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );

        $this->removeFile($path);
    }

    public function test_reading_deep_xml() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(Entry::xml(
                    'node',
                    '<deep id_attribute="1"><leaf id_attribute="1">1</leaf></deep>'
                )),
                Row::create(Entry::xml(
                    'node',
                    '<deep id_attribute="2"><leaf id_attribute="2">2</leaf></deep>'
                )),
                Row::create(Entry::xml(
                    'node',
                    '<deep id_attribute="3"><leaf id_attribute="3">3</leaf></deep>'
                )),
                Row::create(Entry::xml(
                    'node',
                    '<deep id_attribute="4"><leaf id_attribute="4">4</leaf></deep>'
                )),
                Row::create(Entry::xml(
                    'node',
                    '<deep id_attribute="5"><leaf id_attribute="5">5</leaf></deep>'
                )),
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/../Fixtures/deepest_items_flat.xml', 'root/items/item/deep'))
                ->fetch()
        );
    }

    public function test_reading_xml() : void
    {
        $xml = new \DOMDocument();
        $xml->load(__DIR__ . '/../Fixtures/simple_items.xml');

        $this->assertEquals(
            (new Rows(Row::create(Entry::xml('node', $xml)))),
            (new Flow())
                ->read(XML::from(__DIR__ . '/../Fixtures/simple_items.xml'))
                ->fetch()
        );
    }

    public function test_reading_xml_each_collection_item() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(Entry::xml('node', '<item item_attribute_01="1"><id id_attribute_01="1">1</id></item>')),
                Row::create(Entry::xml('node', '<item item_attribute_01="2"><id id_attribute_01="2">2</id></item>')),
                Row::create(Entry::xml('node', '<item item_attribute_01="3"><id id_attribute_01="3">3</id></item>')),
                Row::create(Entry::xml('node', '<item item_attribute_01="4"><id id_attribute_01="4">4</id></item>')),
                Row::create(Entry::xml('node', '<item item_attribute_01="5"><id id_attribute_01="5">5</id></item>')),
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/../Fixtures/simple_items_flat.xml', 'root/items/item'))
                ->fetch()
        );
    }

    public function test_reading_xml_from_path() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML(<<<'XML'
<?xml version="1.0"?>
<items items_attribute_01="1" items_attribute_02="2">
        <item item_attribute_01="1">
            <id id_attribute_01="1">1</id>
        </item>
        <item item_attribute_01="2">
            <id id_attribute_01="2">2</id>
        </item>
        <item item_attribute_01="3">
            <id id_attribute_01="3">3</id>
        </item>
        <item item_attribute_01="4">
            <id id_attribute_01="4">4</id>
        </item>
        <item item_attribute_01="5">
            <id id_attribute_01="5">5</id>
        </item>
    </items>

XML);
        $this->assertEquals(
            new Rows(Row::create(Entry::xml('node', $xml))),
            (new Flow())
                ->read(XML::from(__DIR__ . '/../Fixtures/simple_items.xml', 'root/items'))
                ->fetch()
        );
    }

    public function test_signal_stop() : void
    {
        $path = $this->createTemporaryFile('xml_extractor_signal_stop', '.csv');

        \file_put_contents($path, <<<'XML'
<?xml version="1.0"?>
<items items_attribute_01="1" items_attribute_02="2">
        <item item_attribute_01="1">
            <id id_attribute_01="1">1</id>
        </item>
        <item item_attribute_01="2">
            <id id_attribute_01="2">2</id>
        </item>
        <item item_attribute_01="3">
            <id id_attribute_01="3">3</id>
        </item>
        <item item_attribute_01="4">
            <id id_attribute_01="4">4</id>
        </item>
        <item item_attribute_01="5">
            <id id_attribute_01="5">5</id>
        </item>
    </items>

XML);

        $extractor = new XMLReaderExtractor(Path::realpath($path), 'items/item');

        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame('1', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame('2', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame('3', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());

        $this->removeFile($path);
    }
}
