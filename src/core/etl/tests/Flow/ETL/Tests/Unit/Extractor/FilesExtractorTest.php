<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\FilesExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\UnsizedFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class FilesExtractorTest extends FlowTestCase
{
    public function test_it_declares_one_row_per_listed_file_and_their_bytes_exactly(): void
    {
        $statistics = files('memory://dir/*', MemoryFiles::with([
            'memory://dir/a.txt' => 'abcdefghij',
            'memory://dir/b.txt' => 'abcde',
        ]))->statistics();

        static::assertEquals(Cardinality::exact(2), $statistics->rows);
        static::assertEquals(Cardinality::exact(15), $statistics->size);
    }

    public function test_an_empty_listing_declares_zero_rows_and_zero_bytes(): void
    {
        $statistics = files('memory://dir/*', MemoryFiles::with([]))->statistics();

        static::assertEquals(Cardinality::exact(0), $statistics->rows);
        static::assertEquals(Cardinality::exact(0), $statistics->size);
    }

    public function test_a_member_without_a_size_makes_the_size_unknown(): void
    {
        $statistics = files(
            'memory://dir/*',
            new UnsizedFilesystem(MemoryFiles::with([
                'memory://dir/a.txt' => 'abcdefghij',
                'memory://dir/b.txt' => 'abcde',
            ]), ['memory://dir/b.txt']),
        )->statistics();

        static::assertEquals(Cardinality::exact(2), $statistics->rows);
        static::assertEquals(Cardinality::unknown(), $statistics->size);
    }

    public function test_the_listing_is_read_once(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with(['memory://dir/a.txt' => 'abc']));
        $extractor = files('memory://dir/*', $filesystem);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_a_zero_string_extension_is_not_null(): void
    {
        $batches = iterator_to_array(files(__DIR__ . '/Fixtures/ZeroExtension/*')->extract(flow_context()));

        static::assertSame('0', $batches[0]->first()->get('extension'));
    }

    public function test_partition_directories_become_string_columns(): void
    {
        $extractor = files(__DIR__
        . '/../../Integration/DataFrame/Fixtures/Partitioning/multi_partition_pruning_test/**/*.txt');
        $partitions = schema(str_schema('day'), str_schema('month'), str_schema('year'));

        $first = iterator_to_array($extractor->extract(flow_context()), false)[0]->first();

        static::assertEquals($partitions, $extractor->partitionSchema());
        static::assertEquals($partitions, $extractor->schema()->keep('day', 'month', 'year'));
        static::assertSame(['day' => '30', 'month' => '12', 'year' => '2022'], [
            'day' => $first->get('day'),
            'month' => $first->get('month'),
            'year' => $first->get('year'),
        ]);
    }

    public function test_a_listing_without_partition_directories_declares_no_partition_columns(): void
    {
        static::assertEquals(new Schema(), files(__DIR__ . '/Fixtures/FileListExtractor/*')->partitionSchema());
    }

    public function test_a_declared_schema_still_gets_the_partition_columns(): void
    {
        $extractor = files(__DIR__
        . '/../../Integration/DataFrame/Fixtures/Partitioning/multi_partition_pruning_test/**/*.txt')->withSchema(schema(str_schema(
            'path',
        )));

        static::assertEquals(
            schema(str_schema('path'), str_schema('day'), str_schema('month'), str_schema('year')),
            $extractor->schema(),
        );
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

        self::assertExtractedRowsCount(2, $extractor, limit: 2);
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
