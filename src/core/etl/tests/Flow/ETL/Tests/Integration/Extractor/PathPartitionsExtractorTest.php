<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use function Flow\ETL\DSL\{flow_context, from_path_partitions, rows};
use function Flow\Types\DSL\type_string;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Path;

final class PathPartitionsExtractorTest extends FlowIntegrationTestCase
{
    public function test_extracting_data_from_path_partitions() : void
    {
        $extractor = from_path_partitions(Path::realpath(__DIR__ . '/Fixtures/multi_partitioned/**/*'));

        $extractedData = \iterator_to_array($extractor->extract(flow_context()));

        $rows = rows();

        foreach ($extractedData as $nextRows) {
            $rows = $rows->merge($nextRows);
        }

        self::assertSame(7, $rows->count());

        $actualData = $rows->toArray();

        // Normalize file paths to handle Windows vs Unix file URI differences
        foreach ($actualData as &$item) {
            $item['path'] = \str_replace('\\', '/', type_string()->assert($item['path']));
            $item['path'] = \preg_replace('#^file://+#', 'file:/', $item['path']);
        }

        self::assertEquals(
            [
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2022/month=12/day=30/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '30'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2022/month=12/day=31/file.txt',
                    'partitions' => ['year' => '2022', 'month' => '12', 'day' => '31'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2023/month=1/day=1/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '1'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2023/month=1/day=2/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '2'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2023/month=1/day=3/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '3'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2023/month=1/day=4/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '4'],
                ],
                [
                    'path' => 'file:/' . \str_replace('\\', '/', __DIR__) . '/Fixtures/multi_partitioned/year=2023/month=1/day=5/file.txt',
                    'partitions' => ['year' => '2023', 'month' => '1', 'day' => '5'],
                ],
            ],
            $actualData
        );
    }
}
