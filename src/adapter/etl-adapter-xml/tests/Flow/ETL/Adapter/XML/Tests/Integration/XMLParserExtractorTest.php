<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_keys;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;

final class XMLParserExtractorTest extends FlowIntegrationTestCase
{
    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(xml_schema('node'));

        $extractor = from_xml(__DIR__ . '/../Fixtures/cross_stream/*/file.xml', 'root/item')
            ->withSchema($schema)
            ->withMetadataColumns(true);

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_limit(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'))->withXMLNodePath('root/row');
        $extractor->changeLimit(2);

        $rows = df()->extract($extractor)->fetch()->toArray();

        static::assertCount(2, $rows);
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        $rows = df()
            ->read(from_xml(__DIR__ . '/../Fixtures/cross_stream/*/file.xml', 'root/item'))
            ->fetch()
            ->toArray();

        static::assertSame(['node', 'date'], array_keys($rows[0]));
        static::assertSame('2026-01-01', $rows[0]['date']);
        static::assertSame(['node', 'date'], array_keys($rows[1]));
        static::assertNull($rows[1]['date']);
    }

    public function test_reading_deep_xml(): void
    {
        static::assertSame(
            5,
            df()
                ->read(from_xml(__DIR__ . '/../Fixtures/deepest_items_flat.xml', 'root/items/item/deep'))
                ->fetch()
                ->count(),
        );
    }

    public function test_reading_xml(): void
    {
        static::assertSame(
            1,
            df()
                ->read(from_xml(__DIR__ . '/../Fixtures/simple_items.xml'))
                ->fetch()
                ->count(),
        );
    }

    public function test_reading_xml_each_collection_item(): void
    {
        static::assertXmlStringEqualsXmlString(<<<'XML'
            <item item_attribute_01="1">
              <id id_attribute_01="1">1</id>
            </item>
            XML, type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/simple_items_flat.xml', 'root/items/item'))
            ->fetch()[0]->get('node')));

        static::assertXmlStringEqualsXmlString(<<<'XML'
            <item item_attribute_01="5">
              <id id_attribute_01="5">5</id>
            </item>
            XML, type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/simple_items_flat.xml', 'root/items/item'))
            ->fetch()[4]->get('node')));
    }

    public function test_reading_xml_from_path(): void
    {
        static::assertXmlStringEqualsXmlString(<<<'XML'
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
            XML, type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/simple_items.xml', 'root/items'))
            ->fetch()[0]->get('node')));
    }

    public function test_reading_xml_with_ancestor_namespace_declaration(): void
    {
        $rows = df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_feed.xml', 'feed/entry'))
            ->withEntry('title', ref('node')->xpath('/entry/g:title')->domElementValue())
            ->fetch();

        static::assertSame(['Product 1', 'Product 2'], [$rows[0]->get('title'), $rows[1]->get('title')]);

        $node = type_string()->cast($rows[0]->get('node'));

        static::assertStringContainsString('xmlns:g="http://base.google.com/ns/1.0"', $node);
        static::assertStringContainsString('xmlns:c="http://example.com/custom"', $node);
    }

    public function test_reading_xml_with_default_namespace_declaration(): void
    {
        $node = type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_default.xml', 'feed/entry'))
            ->fetch()[0]->get('node'));

        static::assertStringContainsString('xmlns="http://example.com/default"', $node);
    }

    public function test_reading_xml_with_multi_ancestor_namespace_merge(): void
    {
        $node = type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_multi_ancestor.xml', 'feed/group/entry'))
            ->fetch()[0]->get('node'));

        static::assertStringContainsString('xmlns:a="http://example.com/a"', $node);
        static::assertStringContainsString('xmlns:b="http://example.com/b"', $node);
    }

    public function test_reading_xml_with_namespace_on_captured_root(): void
    {
        $rows = df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_on_captured_root.xml', 'feed/entry'))
            ->withEntry('title', ref('node')->xpath('/entry/g:title')->domElementValue())
            ->fetch();

        static::assertSame('Product 1', $rows[0]->get('title'));
        static::assertStringContainsString(
            'xmlns:g="http://base.google.com/ns/1.0"',
            type_string()->cast($rows[0]->get('node')),
        );
    }

    public function test_reading_xml_with_prefixed_attribute(): void
    {
        $node = type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_prefixed_attribute.xml', 'feed/entry'))
            ->fetch()[0]->get('node'));

        static::assertStringContainsString('xmlns:g="http://base.google.com/ns/1.0"', $node);
        static::assertStringContainsString('xml:lang="en"', $node);
        static::assertStringContainsString('g:priority="high"', $node);
    }

    public function test_reading_xml_with_schema(): void
    {
        $rows = df()
            ->extract(from_xml(__DIR__ . '/../Fixtures/simple_items.xml')->withSchema(schema(
                xml_schema('node'),
                xml_schema('missing', nullable: true),
            )))
            ->fetch()
            ->toArray();

        foreach ($rows as $row) {
            static::assertNotSame([], $row);
            static::assertNotNull($row['node']);
            static::assertNull($row['missing']);
        }
    }

    /**
     * b57: the document has no such node, so every row would carry a null under a NOT NULL
     * declaration.
     */
    public function test_reading_xml_refuses_a_not_null_column_the_document_lacks(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "missing" (row 0) declared by the schema is missing from the row');

        df()
            ->extract(from_xml(__DIR__ . '/../Fixtures/simple_items.xml')->withSchema(schema(
                xml_schema('node'),
                xml_schema('missing'),
            )))
            ->fetch();
    }

    public function test_reading_xml_with_shadowed_namespace_declaration(): void
    {
        $node = type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_nested.xml', 'feed/group/entry'))
            ->fetch()[0]->get('node'));

        static::assertStringContainsString('xmlns:g="http://example.com/override"', $node);
        static::assertStringContainsString('xmlns:x="http://example.com/extra"', $node);
        static::assertStringNotContainsString('http://base.google.com/ns/1.0', $node);
    }

    public function test_reading_xml_with_sibling_namespace_isolation(): void
    {
        $rows = df()
            ->read(from_xml(__DIR__ . '/../Fixtures/namespaced_sibling_isolation.xml', 'feed/entry'))
            ->fetch()
            ->toArray();

        $firstNode = type_string()->cast($rows[0]['node']);
        $secondNode = type_string()->cast($rows[1]['node']);

        static::assertStringContainsString('xmlns:g="http://example.com/first"', $firstNode);
        static::assertStringNotContainsString('xmlns:g', $secondNode);
        static::assertStringNotContainsString('http://example.com/first', $secondNode);
    }

    public function test_reading_xml_without_namespaces_is_unchanged(): void
    {
        static::assertXmlStringEqualsXmlString(<<<'XML'
            <item item_attribute_01="1">
              <id id_attribute_01="1">1</id>
            </item>
            XML, type_string()->cast(df()
            ->read(from_xml(__DIR__ . '/../Fixtures/simple_items_flat.xml', 'root/items/item'))
            ->fetch()[0]->get('node')));
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(xml_schema('node'), str_schema('_input_file_uri')),
            from_xml(__DIR__ . '/../Fixtures/flow_orders.xml')->withMetadataColumns(true)->schema(),
        );
    }

    public function test_schema_defaults_to_a_single_node_column(): void
    {
        static::assertEquals(schema(xml_schema('node')), from_xml(__DIR__ . '/../Fixtures/flow_orders.xml')->schema());
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            from_xml(__DIR__ . '/../Fixtures/flow_orders.xml')->withSchema(schema(str_schema('name')))->schema(),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../Fixtures/flow_orders.xml'))->withXMLNodePath('root/row');

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
