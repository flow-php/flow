<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use function Flow\ETL\Adapter\XML\{from_xml, to_xml};
use function Flow\ETL\DSL\{df, from_array, overwrite};
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class XMLLoaderTest extends IntegrationTestCase
{
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
