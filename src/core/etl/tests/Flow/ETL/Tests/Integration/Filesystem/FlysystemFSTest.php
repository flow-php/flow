<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\DSL\Partitions;
use Flow\ETL\Filesystem\FlysystemFS;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\NoopFilter;
use PHPUnit\Framework\TestCase;

final class FlysystemFSTest extends TestCase
{
    public function test_open_file_stream_for_existing_file() : void
    {
        $stream = (new FlysystemFS())->open(new Path(__FILE__), Mode::READ);

        $this->assertIsResource($stream->resource());
        $this->assertSame(
            \file_get_contents(__FILE__),
            \stream_get_contents($stream->resource())
        );
    }

    public function test_open_file_stream_for_non_existing_file() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_test_file_', true) . '.txt';

        $stream = (new FlysystemFS())->open(new Path($path), Mode::WRITE);

        $this->assertIsResource($stream->resource());
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $paths = \iterator_to_array(
            (new FlysystemFS())
                ->scan(
                    new Path(__DIR__ . '/Fixtures/multi_partitions'),
                    Partitions::chain(
                        Partitions::only('country', 'pl'),
                        Partitions::date_between('date', new \DateTimeImmutable('2022-01-02'), new \DateTimeImmutable('2022-01-04'))
                    )
                )
        );
        \sort($paths);

        $this->assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt'),
                new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt'),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder() : void
    {
        $paths = \iterator_to_array((new FlysystemFS())->scan(new Path(__DIR__ . '/Fixtures/partitioned'), new NoopFilter()));
        \sort($paths);

        $this->assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'),
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder_with_partitions_filtering() : void
    {
        $this->assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'),
            ],
            \iterator_to_array(
                (new FlysystemFS())
                    ->scan(
                        new Path(__DIR__ . '/Fixtures/partitioned'),
                        Partitions::only('partition_01', 'b')
                    )
            )
        );
    }

    public function test_reading_partitioned_folder_with_pattern() : void
    {
        $paths = \iterator_to_array((new FlysystemFS())->scan(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'), new NoopFilter()));
        \sort($paths);

        $this->assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'),
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'),
            ],
            $paths
        );
    }
}
