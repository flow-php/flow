<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use Flow\ETL\Adapter\XML\XMLParserExtractor;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\XML\from_xml;

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
}
