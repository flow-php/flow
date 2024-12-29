<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, Flow, FlowContext, PHP\Type\Caster, Tests\FlowIntegrationTestCase};
use Flow\Filesystem\Path;

final class XMLReaderExtractorTest extends FlowIntegrationTestCase
{
    public function test_limit() : void
    {
        $extractor = new XMLReaderExtractor(Path::realpath(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_reading_deep_xml() : void
    {
        self::assertEquals(
            5,
            (new Flow())
                ->read(new XMLReaderExtractor(new Path(__DIR__ . '/../Fixtures/deepest_items_flat.xml'), 'root/items/item/deep'))
                ->fetch()
                ->count()
        );
    }

    public function test_reading_xml() : void
    {
        $xml = new \DOMDocument();
        $xml->load(__DIR__ . '/../Fixtures/simple_items.xml');

        self::assertEquals(
            1,
            (new Flow())
                ->read(new XMLReaderExtractor(new Path(__DIR__ . '/../Fixtures/simple_items.xml')))
                ->fetch()
                ->count()
        );
    }

    public function test_reading_xml_each_collection_item() : void
    {
        self::assertXmlStringEqualsXmlString(
            <<<'XML'
<item item_attribute_01="1">
  <id id_attribute_01="1">1</id>
</item>
XML,
            Caster::default()->to(type_string())->value(
                (new Flow())
                    ->read(new XMLReaderExtractor(new Path(__DIR__ . '/../Fixtures/simple_items_flat.xml'), 'root/items/item'))
                    ->fetch()[0]
                    ->valueOf('node')
            )
        );

        self::assertXmlStringEqualsXmlString(
            <<<'XML'
<item item_attribute_01="5">
  <id id_attribute_01="5">5</id>
</item>
XML,
            Caster::default()->to(type_string())->value(
                (new Flow())
                    ->read(new XMLReaderExtractor(new Path(__DIR__ . '/../Fixtures/simple_items_flat.xml'), 'root/items/item'))
                    ->fetch()[4]
                    ->valueOf('node')
            )
        );
    }

    public function test_reading_xml_from_path() : void
    {
        self::assertXmlStringEqualsXmlString(
            <<<'XML'
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
XML,
            Caster::default()->to(type_string())->value(
                (new Flow())
                    ->read(new XMLReaderExtractor(new Path(__DIR__ . '/../Fixtures/simple_items.xml'), 'root/items'))
                    ->fetch()[0]->valueOf('node')
            )
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new XMLReaderExtractor(Path::realpath(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');

        $generator = $extractor->extract(new FlowContext(Config::default()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
