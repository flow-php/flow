<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class XMLReaderExtractorTest extends TestCase
{
    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/xml_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

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

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_reading_deep_xml() : void
    {
        self::assertEquals(
            new Rows(
                Row::create(xml_entry(
                    'node',
                    '<deep id_attribute="1"><leaf id_attribute="1">1</leaf></deep>'
                )),
                Row::create(xml_entry(
                    'node',
                    '<deep id_attribute="2"><leaf id_attribute="2">2</leaf></deep>'
                )),
                Row::create(xml_entry(
                    'node',
                    '<deep id_attribute="3"><leaf id_attribute="3">3</leaf></deep>'
                )),
                Row::create(xml_entry(
                    'node',
                    '<deep id_attribute="4"><leaf id_attribute="4">4</leaf></deep>'
                )),
                Row::create(xml_entry(
                    'node',
                    '<deep id_attribute="5"><leaf id_attribute="5">5</leaf></deep>'
                )),
            ),
            (new Flow())
                ->read(from_xml(__DIR__ . '/../Fixtures/deepest_items_flat.xml', 'root/items/item/deep'))
                ->fetch()
        );
    }

    public function test_reading_xml() : void
    {
        $xml = new \DOMDocument();
        $xml->load(__DIR__ . '/../Fixtures/simple_items.xml');

        self::assertEquals(
            (new Rows(Row::create(xml_entry('node', $xml)))),
            (new Flow())
                ->read(from_xml(__DIR__ . '/../Fixtures/simple_items.xml'))
                ->fetch()
        );
    }

    public function test_reading_xml_each_collection_item() : void
    {
        self::assertEquals(
            new Rows(
                Row::create(xml_entry('node', '<item item_attribute_01="1"><id id_attribute_01="1">1</id></item>')),
                Row::create(xml_entry('node', '<item item_attribute_01="2"><id id_attribute_01="2">2</id></item>')),
                Row::create(xml_entry('node', '<item item_attribute_01="3"><id id_attribute_01="3">3</id></item>')),
                Row::create(xml_entry('node', '<item item_attribute_01="4"><id id_attribute_01="4">4</id></item>')),
                Row::create(xml_entry('node', '<item item_attribute_01="5"><id id_attribute_01="5">5</id></item>')),
            ),
            (new Flow())
                ->read(from_xml(__DIR__ . '/../Fixtures/simple_items_flat.xml', 'root/items/item'))
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
        self::assertEquals(
            new Rows(Row::create(xml_entry('node', $xml))),
            (new Flow())
                ->read(from_xml(__DIR__ . '/../Fixtures/simple_items.xml', 'root/items'))
                ->fetch()
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/xml_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

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

        self::assertSame('1', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertSame('2', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertSame('3', $generator->current()->first()->valueOf('node')->getElementsByTagName('id')[0]->nodeValue);
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
