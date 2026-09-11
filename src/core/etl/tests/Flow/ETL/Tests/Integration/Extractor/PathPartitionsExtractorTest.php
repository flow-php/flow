<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Extractor\PathPartitionsExtractor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;
use function str_replace;
use function usort;

final class PathPartitionsExtractorTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

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
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2022/month=12/day=31/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '31'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=1/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '1'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=2/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '2'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=3/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '3'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=4/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '4'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=5/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '5'],
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
