<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function count;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_instance_of;

final class JsonSplitReadTest extends FlowTestCase
{
    public function test_the_document_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $extractor = from_json(path_real(__DIR__ . '/../../../Fixtures/cross_stream/*/data.json'))->withBatchSize(1);
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

    public function test_the_lines_reader_with_a_pointer_rebuilds_the_same(): void
    {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../../Fixtures/five_rows.jsonl'))->withBatchSize(2);
        $reader = $extractor->reader();

        static::assertSame(
            ExtractedRows::of($extractor)->toArray(),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context()),
        );
        static::assertSame([2, 2, 1], ReaderRoundTrip::batches(DescribeRoundTrip::rebuilt($reader), flow_context()));
    }

    /**
     * @param int<1, max> $targetSplitBytes
     */
    #[TestWith(['timezones.jsonl', 4096, null])]
    #[TestWith(['timezones.jsonl', 100, null])]
    #[TestWith(['crlf_blank_line.jsonl', 8, null])]
    #[TestWith(['pointer_lines.jsonl', 8, '/items'])]
    public function test_the_lines_reader_reads_a_file_by_windows_as_it_reads_it_whole(
        string $fixture,
        int $targetSplitBytes,
        ?string $pointer,
    ): void {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../../Fixtures/' . $fixture))->withBatchSize(7);

        if ($pointer !== null) {
            $extractor->withPointer($pointer);
        }

        $splits = $extractor->splits(new Criteria(), $targetSplitBytes);
        $rows = [];

        foreach ($splits as $split) {
            foreach (DescribeRoundTrip::rebuilt($extractor->reader())->extract(
                flow_context()->backend(),
                (new Criteria())->withSplit(type_instance_of(FileSplit::class)->assert($split)),
            ) as $batch) {
                $rows = [...$rows, ...$batch->toArray()];
            }
        }

        static::assertGreaterThan(1, count($splits));
        static::assertSame(ExtractedRows::of($extractor)->toArray(), $rows);
    }

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(from_json(path('memory://a.json'), filesystem: memory_filesystem())->reader()->describe());
    }

    public function test_the_reader_declares_json(): void
    {
        static::assertSame(
            ['json'],
            from_json(path_real(__DIR__ . '/../../../Fixtures/cross_stream/*/data.json'))
                ->reader()
                ->describe()
                ?->extensions(),
        );
    }
}
