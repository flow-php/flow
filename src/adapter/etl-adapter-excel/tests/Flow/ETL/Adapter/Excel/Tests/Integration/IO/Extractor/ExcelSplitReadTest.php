<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\IO\Extractor;

use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\Filesystem\DSL\path_real;

final class ExcelSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $extractor = from_excel(path_real(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx'))->withBatchSize(3);
        $reader = $extractor->reader();

        static::assertSame(ExtractedRows::of($extractor)->toArray(), ReaderRoundTrip::rows($reader, flow_context()));
        static::assertSame(
            ReaderRoundTrip::extractorBatches($extractor, flow_context()),
            ReaderRoundTrip::batches($reader, flow_context()),
        );
        static::assertSame(
            ReaderRoundTrip::rows($reader, flow_context()),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context()),
        );
    }

    public function test_a_rebuilt_reader_carries_no_sample_and_parses_afresh(): void
    {
        $extractor = from_excel(path_real(__DIR__ . '/../../../Fixtures/sniff/a'))
            ->inferSchema(infer_schema()->sampleSize(1));
        $extractor->schema();

        static::assertCount(10, ReaderRoundTrip::rows(
            DescribeRoundTrip::rebuilt($extractor->reader()),
            flow_context(),
        ));
        static::assertCount(10, ReaderRoundTrip::rows($extractor->reader(), flow_context()));
    }

    public function test_the_reader_declares_the_extensions_a_workbook_is_read_with(): void
    {
        static::assertSame(
            ['dom', 'filter', 'xmlreader', 'zip'],
            from_excel(path_real(__DIR__ . '/../../../Fixtures/partitioned/group=*/*.xlsx'))
                ->reader()
                ->describe()
                ?->extensions(),
        );
    }
}
