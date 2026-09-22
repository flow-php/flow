<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\PathPartitionsExtractor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function str_replace;
use function usort;

final class PathPartitionsExtractorTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

    public function test_it_declares_one_row_per_listed_file(): void
    {
        $statistics = from_path_partitions('memory://dir/**/*', MemoryFiles::with([
            'memory://dir/year=2024/a.txt' => 'a',
            'memory://dir/year=2024/b.txt' => 'b',
            'memory://dir/year=2025/c.txt' => 'c',
        ]))->statistics();

        static::assertEquals(Cardinality::exact(3), $statistics->rows);
        static::assertEquals(Cardinality::unknown(), $statistics->size);
    }

    public function test_an_empty_listing_declares_zero_rows(): void
    {
        static::assertEquals(
            Cardinality::exact(0),
            from_path_partitions('memory://dir/**/*', MemoryFiles::with([]))->statistics()->rows,
        );
    }

    public function test_the_listing_is_read_once(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with(['memory://dir/year=2024/a.txt' => 'a']));
        $extractor = from_path_partitions('memory://dir/**/*', $filesystem);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_partition_directories_are_declared_as_string_columns_next_to_the_map(): void
    {
        $extractor = from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'));

        static::assertEquals(
            schema(str_schema('day'), str_schema('month'), str_schema('year')),
            $extractor->partitionSchema(),
        );
        static::assertEquals(
            schema(
                str_schema('path'),
                map_schema('partitions', type_map(type_string(), type_string())),
                str_schema('day'),
                str_schema('month'),
                str_schema('year'),
            ),
            $extractor->schema(),
        );
    }

    public function test_extracting_data_from_path_partitions(): void
    {
        $extractor = from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'));

        $extractedData = iterator_to_array($extractor->extract(flow_context()));

        $rows = rows(schema());

        foreach ($extractedData as $nextRows) {
            static::assertInstanceOf(Rows::class, $nextRows);
            $rows = $rows->merge($nextRows);
        }

        static::assertSame(7, $rows->count());

        $actualData = $rows->toArray();
        usort($actualData, static fn(array $a, array $b): int => (string) $a['path'] <=> (string) $b['path']);

        static::assertEquals(
            [
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2022/month=12/day=30/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '30'],
                    'day' => '30',
                    'month' => '12',
                    'year' => '2022',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2022/month=12/day=31/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '31'],
                    'day' => '31',
                    'month' => '12',
                    'year' => '2022',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=1/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '1'],
                    'day' => '1',
                    'month' => '1',
                    'year' => '2023',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=2/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '2'],
                    'day' => '2',
                    'month' => '1',
                    'year' => '2023',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=3/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '3'],
                    'day' => '3',
                    'month' => '1',
                    'year' => '2023',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=4/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '4'],
                    'day' => '4',
                    'month' => '1',
                    'year' => '2023',
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=5/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '5'],
                    'day' => '5',
                    'month' => '1',
                    'year' => '2023',
                ],
            ],
            $actualData,
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'))->isRepeatable(),
        );
    }

    public function test_batches_at_the_configured_batch_size(): void
    {
        $sizes = [];

        foreach (from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'))
            ->withBatchSize(2)
            ->extract(flow_context()) as $rows) {
            $sizes[] = $rows->count();
        }

        static::assertSame([2, 2, 2, 1], $sizes);
    }

    public function test_path_partitions_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): PathPartitionsExtractor => from_path_partitions(path_real(__DIR__
            . '/Fixtures/multi_partitioned/**/*')),
            ExtractedRows::of(
                from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'))->withBatchSize(1),
            ),
        );
    }
}
