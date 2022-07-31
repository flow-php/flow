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
    public function test_append_mode() : void
    {
        $fs = new FlysystemFS();

        $stream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/append.txt'), Mode::APPEND);
        \fwrite($stream->resource(), "some data to make file not empty\n");
        $stream->close();

        $appendStream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/append.txt'), Mode::APPEND);
        \fwrite($appendStream->resource(), "some more data to make file not empty\n");
        $appendStream->close();

        $this->assertStringContainsString(
            <<<'STRING'
some data to make file not empty
some more data to make file not empty
STRING,
            \file_get_contents($appendStream->path()->path())
        );

        $fs->rm($stream->path());
        $this->assertFalse($fs->exists($stream->path()));
    }

    public function test_dir_exists() : void
    {
        $this->assertTrue((new FlysystemFS())->exists(new Path(__DIR__)));
        $this->assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists() : void
    {
        $this->assertTrue((new FlysystemFS())->exists(new Path(__FILE__)));
        $this->assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists() : void
    {
        $this->assertTrue((new FlysystemFS())->exists(new Path(__DIR__ . '/**/*.txt')));
        $this->assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/**/*.pdf')));
    }

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

    public function test_remove_directory_with_content_when_exists() : void
    {
        $fs = new FlysystemFS();

        $dirPath = Path::realpath(\sys_get_temp_dir() . '/flow-fs-test-directory/');

        $stream = $fs->open(Path::realpath($dirPath->path() . '/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        $this->assertTrue($fs->exists($dirPath));
        $this->assertTrue($fs->exists($stream->path()));
        $fs->rm($dirPath);
        $this->assertFalse($fs->exists($dirPath));
        $this->assertFalse($fs->exists($stream->path()));
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = new FlysystemFS();

        $stream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        $this->assertTrue($fs->exists($stream->path()));
        $fs->rm($stream->path());
        $this->assertFalse($fs->exists($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = new FlysystemFS();

        $dirPath = Path::realpath(\sys_get_temp_dir() . '/flow-fs-test-directory/');

        $stream = $fs->open(Path::realpath($dirPath->path() . '/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        $this->assertTrue($fs->exists($dirPath));
        $this->assertTrue($fs->exists($stream->path()));
        $fs->rm(Path::realpath($dirPath->path() . '/*.txt'));
        $this->assertTrue($fs->exists($dirPath));
        $this->assertFalse($fs->exists($stream->path()));
        $fs->rm($dirPath);
    }
}
