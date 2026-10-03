<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use DOMDocument;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Config;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_xml;

final class XMLExtractorTypedColumnsTest extends FlowTestCase
{
    public function test_reads_each_node_into_a_typed_xml_column(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))->withXMLNodePath('root/items/item');

        $rows = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            static::assertEquals(type_xml(), $batch->schema()->get('node')->type());

            $rows = [...$rows, ...$batch->toArray()];
        }

        static::assertCount(5, $rows);
    }

    public function test_appends_input_file_uri_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_xml($path = path_real(__DIR__ . '/../Fixtures/simple_items.xml'))
            ->withXMLNodePath('root/items/item')
            ->withMetadataColumns(true);

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            static::assertSame([$path->uri()], array_values(array_unique($batch->column('_input_file_uri')->values())));
        }
    }

    public function test_an_xml_node_column_holds_the_utf8_document_of_each_node(): void
    {
        $physicals = [];

        foreach (from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))
            ->withXMLNodePath('root/items/item')
            ->extract(flow_context()) as $batch) {
            $physicals = [...$physicals, ...$batch->column('node')->physicals()];
        }

        static::assertCount(5, $physicals);
        static::assertSame(
            XmlDocumentPhysical::DECLARATION
            . "<item item_attribute_01=\"1\">\n            <id id_attribute_01=\"1\">1</id>\n        </item>\n",
            $physicals[0],
        );
    }

    public function test_a_string_node_column_holds_each_node_text(): void
    {
        $rows = [];

        foreach (from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))
            ->withXMLNodePath('root/items/item')
            ->withSchema(schema(str_schema('node')))
            ->extract(flow_context()) as $batch) {
            $rows = [...$rows, ...$batch->toArray()];
        }

        static::assertCount(5, $rows);
        static::assertSame(
            ['node' => "<item item_attribute_01=\"5\">\n            <id id_attribute_01=\"5\">5</id>\n        </item>"],
            $rows[4],
        );
    }

    public function test_an_xml_node_column_beside_other_columns_reads_the_node_documents(): void
    {
        $nodes = [];

        foreach (from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))
            ->withXMLNodePath('root/items/item')
            ->withSchema(schema(xml_schema('node'), str_schema('missing', nullable: true)))
            ->extract(flow_context()) as $batch) {
            $nodes = [...$nodes, ...$batch->column('node')->values()];
        }

        static::assertCount(5, $nodes);
        static::assertInstanceOf(DOMDocument::class, $nodes[0]);
        static::assertSame('UTF-8', $nodes[0]->encoding);
        static::assertSame(
            "<item item_attribute_01=\"1\">\n            <id id_attribute_01=\"1\">1</id>\n        </item>",
            $nodes[0]->saveXML($nodes[0]->documentElement),
        );
    }
}
