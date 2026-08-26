<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\path_real;

final class XMLHydratorParityTest extends FlowTestCase
{
    public function test_reads_each_node_into_a_typed_xml_entry(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))->withXMLNodePath('root/items/item');

        $rows = [];

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            foreach ($batch as $row) {
                static::assertInstanceOf(XMLEntry::class, $row->get('node'));
                $rows[] = $row;
            }
        }

        static::assertCount(5, $rows);
    }

    public function test_appends_input_file_uri_when_metadata_columns_are_enabled(): void
    {
        $extractor = from_xml($path = path_real(__DIR__ . '/../Fixtures/simple_items.xml'))
            ->withXMLNodePath('root/items/item')
            ->withMetadataColumns(true);

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $batch) {
            foreach ($batch as $row) {
                static::assertTrue($row->has('_input_file_uri'));
                static::assertSame($path->uri(), $row->valueOf('_input_file_uri'));
            }
        }
    }

    public function test_honours_a_hydrator_configured_on_the_context(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../Fixtures/simple_items.xml'))->withXMLNodePath('root/items/item');

        $count = 0;

        foreach ($extractor->extract(
            flow_context(Config::builder()->hydrator(new AdaptiveRowHydrator())->build()),
        ) as $batch) {
            foreach ($batch as $row) {
                static::assertInstanceOf(XMLEntry::class, $row->get('node'));
                $count++;
            }
        }

        static::assertSame(5, $count);
    }
}
