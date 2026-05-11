<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_path_partitions;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path_real;

final class PathPartitionsExtractorTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

    public function test_extracting_data_from_path_partitions(): void
    {
        $extractor = from_path_partitions(path_real(__DIR__ . '/Fixtures/multi_partitioned/**/*'));

        $extractedData = \iterator_to_array($extractor->extract(flow_context()));

        $rows = rows();

        foreach ($extractedData as $nextRows) {
            $rows = $rows->merge($nextRows);
        }

        static::assertSame(7, $rows->count());

        $actualData = $rows->toArray();
        \usort($actualData, static fn(array $a, array $b): int => $a['path'] <=> $b['path']);

        static::assertEquals(
            [
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2022/month=12/day=30/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '30'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2022/month=12/day=31/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '31'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=1/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '1'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=2/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '2'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=3/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '3'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=4/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '4'],
                ],
                [
                    'path' =>
                        'file://'
                            . ltrim(\str_replace('\\', '/', __DIR__), '/')
                            . '/Fixtures/multi_partitioned/year=2023/month=1/day=5/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '5'],
                ],
            ],
            $actualData,
        );
    }
}
