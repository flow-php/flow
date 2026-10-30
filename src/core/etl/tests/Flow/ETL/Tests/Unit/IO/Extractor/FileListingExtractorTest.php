<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\FileListingExtractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\UnsizedFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\files;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FileListingExtractorTest extends FlowTestCase
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
        static::assertSame(
            '0',
            iterator_to_array(files(__DIR__ . '/../../../Fixtures/ZeroExtension/*')->extract(
                flow_context()->backend(),
                new Criteria(),
            ))[0]
                ->column('extension')
                ->value(0),
        );
    }

    public function test_partition_directories_become_string_columns(): void
    {
        $extractor = files(__DIR__ . '/../../../Fixtures/multi_partitioned/**/*.txt');
        $partitions = schema(str_schema('day'), str_schema('month'), str_schema('year'));

        $first = iterator_to_array(
            $extractor->extract(flow_context()->backend(), new Criteria()),
            false,
        )[0]->toArray()[0];

        static::assertEquals($partitions, $extractor->partitionSchema());
        static::assertEquals($partitions, $extractor->schema()->keep('day', 'month', 'year'));
        static::assertSame(['day' => '30', 'month' => '12', 'year' => '2022'], [
            'day' => $first['day'],
            'month' => $first['month'],
            'year' => $first['year'],
        ]);
    }

    public function test_a_listing_without_partition_directories_declares_no_partition_columns(): void
    {
        static::assertEquals(
            new Schema(),
            files(__DIR__ . '/../../../Fixtures/FileListExtractor/*')->partitionSchema(),
        );
    }

    public function test_a_declared_schema_still_gets_the_partition_columns(): void
    {
        static::assertEquals(
            schema(str_schema('path'), str_schema('day'), str_schema('month'), str_schema('year')),
            files(__DIR__ . '/../../../Fixtures/multi_partitioned/**/*.txt')
                ->withSchema(schema(str_schema('path')))
                ->schema(),
        );
    }

    public function test_extracting_files_from_directory(): void
    {
        self::assertExtractedRowsCount(3, files(__DIR__ . '/../../../Fixtures/FileListExtractor/*'));
    }

    public function test_extracting_files_from_directory_after_getting_stop_signal(): void
    {
        $generator = files(__DIR__ . '/../../../Fixtures/FileListExtractor/*')
            ->withBatchSize(1)
            ->extract(flow_context()->backend(), new Criteria());
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
        self::assertExtractedRowsCount(6, files(__DIR__ . '/../../../Fixtures/FileListExtractor/**/*'));
    }

    public function test_extracting_files_from_directory_with_limit(): void
    {
        self::assertExtractedRowsCount(
            2,
            files(__DIR__ . '/../../../Fixtures/FileListExtractor/**/*')->withBatchSize(1),
            criteria: new Criteria(2),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(files(__DIR__ . '/../../../Fixtures/FileListExtractor/*')->isRepeatable());
    }

    public function test_batches_at_the_configured_batch_size(): void
    {
        $sizes = [];

        foreach (files(__DIR__ . '/../../../Fixtures/FileListExtractor/**/*')
            ->withBatchSize(2)
            ->extract(flow_context()->backend(), new Criteria()) as $rows) {
            $sizes[] = $rows->count();
        }

        static::assertSame([2, 2, 2], $sizes);
    }

    public function test_every_row_of_a_batch_carries_its_own_files_partition_value(): void
    {
        $batches = iterator_to_array(
            files('memory://dir/**/*.txt', MemoryFiles::with([
                'memory://dir/year=2023/a.txt' => 'a',
                'memory://dir/year=2024/b.txt' => 'b',
                'memory://dir/year=2025/c.txt' => 'c',
            ]))
                ->withBatchSize(3)
                ->extract(flow_context()->backend(), new Criteria()),
            false,
        );

        static::assertCount(1, $batches);
        static::assertSame(['2023', '2024', '2025'], $batches[0]->column('year')->values());
        static::assertSame(['a.txt', 'b.txt', 'c.txt'], $batches[0]->column('base_name')->values());
    }

    public function test_a_limit_at_the_batch_boundary_keeps_each_rows_partition_value(): void
    {
        $batches = iterator_to_array(
            files('memory://dir/**/*.txt', MemoryFiles::with([
                'memory://dir/year=2023/a.txt' => 'a',
                'memory://dir/year=2024/b.txt' => 'b',
                'memory://dir/year=2025/c.txt' => 'c',
            ]))
                ->withBatchSize(2)
                ->extract(flow_context()->backend(), new Criteria(limit: 2)),
            false,
        );

        static::assertCount(1, $batches);
        static::assertSame(['2023', '2024'], $batches[0]->column('year')->values());
    }

    public function test_files_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): FileListingExtractor => files(__DIR__ . '/../../../Fixtures/FileListExtractor/**/*'),
            ExtractedRows::of(files(__DIR__ . '/../../../Fixtures/FileListExtractor/**/*')->withBatchSize(1)),
        );
    }

    public function test_a_scan_returns_only_the_files_matching_its_filter(): void
    {
        $batches = iterator_to_array(files('memory://dir/*.txt', MemoryFiles::with([
            'memory://dir/a.txt' => 'a',
            'memory://dir/b.txt' => 'b',
            'memory://dir/c.txt' => 'c',
            'memory://dir/d.txt' => 'd',
            'memory://dir/e.txt' => 'e',
        ]))
            ->withBatchSize(2)
            ->extract(flow_context()->backend(), new Criteria(conjuncts: [ref('base_name')->isIn(lit([
                'c.txt',
                'd.txt',
            ]))])), false);

        static::assertCount(1, $batches);
        static::assertSame(['c.txt', 'd.txt'], $batches[0]->column('base_name')->values());
    }

    public function test_extract_split_stops_on_the_stop_signal(): void
    {
        $generator = files(__DIR__ . '/../../../Fixtures/FileListExtractor/*')
            ->withBatchSize(1)
            ->extract(
                flow_context()->backend(),
                (new Criteria())->withSplit(
                    new FileSplit(path(__DIR__ . '/../../../Fixtures/FileListExtractor/*'), 0, null),
                ),
            );
        $totalRows = 0;

        foreach ($generator as $rows) {
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $totalRows);
    }

    public function test_a_split_reads_one_listed_file(): void
    {
        $rows = iterator_to_array(
            files('memory://dir/*', MemoryFiles::with([
                'memory://dir/a.txt' => 'a',
                'memory://dir/b.txt' => 'bb',
            ]))->extract(
                flow_context()->backend(),
                (new Criteria())->withSplit(new FileSplit(path('memory://dir/b.txt'), 0, null)),
            ),
            false,
        );

        static::assertCount(1, $rows);
        static::assertSame('b.txt', $rows[0]->toArray()[0]['base_name']);
    }
}
