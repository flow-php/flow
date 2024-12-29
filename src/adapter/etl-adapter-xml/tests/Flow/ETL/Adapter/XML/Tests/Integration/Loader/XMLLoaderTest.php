<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use function Flow\ETL\Adapter\XML\{from_xml, to_xml};
use function Flow\ETL\DSL\{df, from_array, overwrite, ref};
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class XMLLoaderTest extends FlowIntegrationTestCase
{
    public function test_partitioning_xml_file() : void
    {
        df()
            ->read(from_array($dataset = [
                ['id' => 1, 'color' => 'red', 'size' => 'small'],
                ['id' => 2, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 3, 'color' => 'green', 'size' => 'large'],
                ['id' => 4, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 5, 'color' => 'black', 'size' => 'medium'],
                ['id' => 6, 'color' => 'white', 'size' => 'large'],
                ['id' => 7, 'color' => 'red', 'size' => 'small'],
                ['id' => 8, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 9, 'color' => 'green', 'size' => 'large'],
                ['id' => 10, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 11, 'color' => 'black', 'size' => 'medium'],
                ['id' => 12, 'color' => 'white', 'size' => 'large'],
            ]))
            ->saveMode(overwrite())
            ->batchSize(1)
            ->partitionBy('size', 'color')
            ->write(to_xml($path = __DIR__ . '/var/test_partitioning_xml_file/products.xml'))
            ->run();

        self::assertEquals(
            $dataset,
            df()
                ->read(from_xml(__DIR__ . '/var/test_partitioning_xml_file/**/*.xml')->withXMLNodePath('rows/row'))
                ->withEntry('id', ref('node')->xpath('id')->domElementValue()->cast('int'))
                ->withEntry('color', ref('node')->xpath('color')->domElementValue())
                ->withEntry('size', ref('node')->xpath('size')->domElementValue())
                ->drop('node')
                ->sortBy(ref('id')->asc())
                ->fetch()
                ->toArray()
        );
    }

    public function test_writing_empty_rows() : void
    {
        df()
            ->read(from_array([]))
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml')))
            ->run();

        self::assertFalse(\file_exists($path->path()));
    }

    public function test_writing_xml() : void
    {
        df()
            ->read(new FakeExtractor(100))
            ->saveMode(overwrite())
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml')))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_xml($path, 'rows/row'))->count()
        );
    }

    public function test_writing_xml_with_attributes() : void
    {
        df()
            ->read(from_array([
                ['_id' => 1, 'name' => 'John', 'address' => ['_id' => 1, 'city' => 'New York', 'street' => '5th Avenue']],
                ['_id' => 2, 'name' => 'Jane', 'address' => ['_id' => 2, 'city' => 'Los Angeles', 'street' => 'Hollywood Boulevard']],
            ]))
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml')))
            ->run();

        self::assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<rows>
<row id="1"><name>John</name><address id="1"><city>New York</city><street>5th Avenue</street></address></row>
<row id="2"><name>Jane</name><address id="2"><city>Los Angeles</city><street>Hollywood Boulevard</street></address></row>
</rows>
XML,
            \file_get_contents($path->path())
        );
    }
}
