<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\FilesExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\flow_context;
use function iterator_to_array;

final class FilesExtractorTest extends FlowTestCase
{
    public function test_a_zero_string_extension_is_not_null(): void
    {
        $batches = iterator_to_array(files(__DIR__ . '/Fixtures/ZeroExtension/*')->extract(flow_context()));

        static::assertSame('0', $batches[0]->first()->get('extension'));
    }

    public function test_extracting_files_from_directory(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/*');

        self::assertExtractedRowsCount(3, $extractor);
    }

    public function test_extracting_files_from_directory_after_getting_stop_signal(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/*')->withBatchSize(1);
        $generator = $extractor->extract(flow_context());
        $totalRows = 0;

        foreach ($generator as $rows) {
            static::assertCount(1, $rows);
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertEquals(1, $totalRows);
    }

    public function test_extracting_files_from_directory_recursive(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/**/*');

        self::assertExtractedRowsCount(6, $extractor);
    }

    public function test_extracting_files_from_directory_with_limit(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/**/*')->withBatchSize(1);
        $extractor->pushLimit(2);

        self::assertExtractedRowsCount(2, $extractor);
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(files(__DIR__ . '/Fixtures/FileListExtractor/*')->isRepeatable());
    }

    public function test_batches_at_the_configured_batch_size(): void
    {
        $sizes = [];

        foreach (files(__DIR__ . '/Fixtures/FileListExtractor/**/*')
            ->withBatchSize(2)
            ->extract(flow_context()) as $rows) {
            $sizes[] = $rows->count();
        }

        static::assertSame([2, 2, 2], $sizes);
    }

    public function test_files_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): FilesExtractor => files(__DIR__ . '/Fixtures/FileListExtractor/**/*'),
            ExtractedRows::of(files(__DIR__ . '/Fixtures/FileListExtractor/**/*')->withBatchSize(1)),
        );
    }
}
