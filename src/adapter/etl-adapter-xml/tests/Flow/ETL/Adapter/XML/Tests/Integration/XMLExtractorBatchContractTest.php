<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use Flow\ETL\Adapter\XML\XMLParserExtractor;
use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\Filesystem\DSL\path_real;

final class XMLExtractorBatchContractTest extends FlowTestCase
{
    public function test_xml_parser_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): XMLParserExtractor => from_xml(__DIR__ . '/../Fixtures/simple_items.xml')->withXMLNodePath(
                'root/items/item',
            ),
            // one row per batch is the read the other sizes must reproduce
            ExtractedRows::of(
                from_xml(__DIR__ . '/../Fixtures/simple_items.xml')
                    ->withXMLNodePath('root/items/item')
                    ->withBatchSize(1),
            ),
        );
    }

    public function test_xml_reader_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            // @mago-ignore analysis:deprecated-class
            static fn(): XMLReaderExtractor => new XMLReaderExtractor(
                path_real(__DIR__ . '/../Fixtures/simple_items.xml'),
                'root/items/item',
            ),
            // @mago-ignore analysis:deprecated-class
            ExtractedRows::of((new XMLReaderExtractor(
                path_real(__DIR__ . '/../Fixtures/simple_items.xml'),
                'root/items/item',
            ))->withBatchSize(1)),
        );
    }
}
