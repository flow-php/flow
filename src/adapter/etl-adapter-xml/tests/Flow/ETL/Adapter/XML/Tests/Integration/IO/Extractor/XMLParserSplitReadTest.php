<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\IO\Extractor;

use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

final class XMLParserSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $extractor = from_xml(path_real(__DIR__ . '/../../../Fixtures/flow_orders.xml'))
            ->withXMLNodePath('root/row')
            ->withBatchSize(3);
        $reader = $extractor->reader();

        static::assertEquals(ExtractedRows::of($extractor)->toArray(), ReaderRoundTrip::rows($reader, flow_context()));
        static::assertSame(
            ReaderRoundTrip::extractorBatches($extractor, flow_context()),
            ReaderRoundTrip::batches($reader, flow_context()),
        );
        static::assertEquals(
            ReaderRoundTrip::rows($reader, flow_context()),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context()),
        );
    }

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(from_xml(path('memory://a.xml'), filesystem: memory_filesystem())->reader()->describe());
    }

    public function test_the_reader_declares_the_xml_extensions(): void
    {
        static::assertSame(
            ['dom', 'xml', 'xmlreader', 'xmlwriter'],
            from_xml(path_real(__DIR__ . '/../../../Fixtures/flow_orders.xml'))->reader()->describe()?->extensions(),
        );
    }
}
