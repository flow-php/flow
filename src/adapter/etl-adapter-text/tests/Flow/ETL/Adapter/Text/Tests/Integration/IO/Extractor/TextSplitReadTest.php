<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration\IO\Extractor;

use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

final class TextSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../../../Fixtures/cross_stream/*/data.txt'))->withBatchSize(2);
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

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(from_text(path('memory://a.txt'), memory_filesystem())->reader()->describe());
    }
}
