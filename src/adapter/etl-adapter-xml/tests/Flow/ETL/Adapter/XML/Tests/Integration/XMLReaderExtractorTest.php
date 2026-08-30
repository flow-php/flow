<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use DOMDocument;
use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_keys;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class XMLReaderExtractorTest extends FlowIntegrationTestCase
{
    public function test_limit(): void
    {
        // @mago-ignore analysis:deprecated-class
        $extractor = new XMLReaderExtractor(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');
        $extractor->changeLimit(2);

        static::assertCount(2, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        $rows = data_frame()
            ->read(
                // @mago-ignore analysis:deprecated-class
                new XMLReaderExtractor(path(__DIR__ . '/../Fixtures/cross_stream/*/file.xml'), 'root/item'),
            )
            ->fetch()
            ->toArray();

        static::assertSame(['node', 'date'], array_keys($rows[0]));
        static::assertSame('2026-01-01', $rows[0]['date']);
        static::assertSame(['node', 'date'], array_keys($rows[1]));
        static::assertNull($rows[1]['date']);
    }

    public function test_reading_deep_xml(): void
    {
        static::assertEquals(
            5,
            data_frame()
                ->read(
                    // @mago-ignore analysis:deprecated-class
                    new XMLReaderExtractor(
                        path(__DIR__ . '/../Fixtures/deepest_items_flat.xml'),
                        'root/items/item/deep',
                    ),
                )
                ->fetch()
                ->count(),
        );
    }

    public function test_reading_xml(): void
    {
        $xml = new DOMDocument();
        $xml->load(__DIR__ . '/../Fixtures/simple_items.xml');

        static::assertEquals(
            1,
            data_frame()
                // @mago-ignore analysis:deprecated-class
                ->read(new XMLReaderExtractor(path(__DIR__ . '/../Fixtures/simple_items.xml')))
                ->fetch()
                ->count(),
        );
    }

    public function test_reading_xml_each_collection_item(): void
    {
        // @mago-ignore analysis:deprecated-class
        $extractor1 = new XMLReaderExtractor(path(__DIR__ . '/../Fixtures/simple_items_flat.xml'), 'root/items/item');
        static::assertXmlStringEqualsXmlString(<<<'XML'
            <item item_attribute_01="1">
              <id id_attribute_01="1">1</id>
            </item>
            XML, type_string()->cast(data_frame()->read($extractor1)->fetch()[0]->get('node')));

        // @mago-ignore analysis:deprecated-class
        $extractor2 = new XMLReaderExtractor(path(__DIR__ . '/../Fixtures/simple_items_flat.xml'), 'root/items/item');
        static::assertXmlStringEqualsXmlString(<<<'XML'
            <item item_attribute_01="5">
              <id id_attribute_01="5">5</id>
            </item>
            XML, type_string()->cast(data_frame()->read($extractor2)->fetch()[4]->get('node')));
    }

    public function test_reading_xml_from_path(): void
    {
        static::assertXmlStringEqualsXmlString(
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
            type_string()->cast(data_frame()
                // @mago-ignore analysis:deprecated-class
                ->read(new XMLReaderExtractor(path(__DIR__ . '/../Fixtures/simple_items.xml'), 'root/items'))
                ->fetch()[0]->get('node')),
        );
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        // @mago-ignore analysis:deprecated-class
        $extractor = new XMLReaderExtractor(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');

        static::assertEquals(
            schema(xml_schema('node'), str_schema('_input_file_uri')),
            $extractor->withMetadataColumns(true)->schema(),
        );
    }

    public function test_schema_describes_a_single_node_column(): void
    {
        // @mago-ignore analysis:deprecated-class
        $extractor = new XMLReaderExtractor(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');

        static::assertEquals(schema(xml_schema('node')), $extractor->schema());
    }

    public function test_signal_stop(): void
    {
        // @mago-ignore analysis:deprecated-class
        $extractor = new XMLReaderExtractor(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'), 'root/row');

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }
}
