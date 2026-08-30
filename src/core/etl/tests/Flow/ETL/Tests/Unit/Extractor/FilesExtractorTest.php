<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\Signal;
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
        self::assertExtractedBatchesSize(1, $extractor);
    }

    public function test_extracting_files_from_directory_after_getting_stop_signal(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/*');
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
        self::assertExtractedBatchesSize(1, $extractor);
    }

    public function test_extracting_files_from_directory_with_limit(): void
    {
        $extractor = files(__DIR__ . '/Fixtures/FileListExtractor/**/*');
        $extractor->changeLimit(2);

        self::assertExtractedRowsCount(2, $extractor);
        self::assertExtractedBatchesSize(1, $extractor);
    }
}
