<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\ScalarFunctionFilter;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class LocalFilesystemTest extends TestCase
{
    public function test_append_mode() : void
    {
        $fs = new LocalFilesystem();

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
        $this->assertTrue((new LocalFilesystem())->exists(new Path(__DIR__)));
        $this->assertFalse((new LocalFilesystem())->exists(new Path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists() : void
    {
        $this->assertTrue((new LocalFilesystem())->exists(new Path(__FILE__)));
        $this->assertFalse((new LocalFilesystem())->exists(new Path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists() : void
    {
        $this->assertTrue((new LocalFilesystem())->exists(new Path(__DIR__ . '/**/*.txt')));
        $this->assertFalse((new LocalFilesystem())->exists(new Path(__DIR__ . '/**/*.pdf')));
    }

    public function test_open_file_stream_for_existing_file() : void
    {
        $stream = (new LocalFilesystem())->open(new Path(__FILE__), Mode::READ);

        $this->assertIsResource($stream->resource());
        $this->assertSame(
            \file_get_contents(__FILE__),
            \stream_get_contents($stream->resource())
        );
    }

    public function test_open_file_stream_for_non_existing_file() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_test_file_', true) . '.txt';

        $stream = (new LocalFilesystem())->open(new Path($path), Mode::WRITE);

        $this->assertIsResource($stream->resource());
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $paths = \iterator_to_array(
            (new LocalFilesystem())
                ->scan(
                    new Path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                    new ScalarFunctionFilter(
                        all(
                            ref('country')->equals(lit('pl')),
                            all(
                                ref('date')->cast('date')->greaterThanEqual(lit(new \DateTimeImmutable('2022-01-02'))),
                                ref('date')->cast('date')->lessThan(lit(new \DateTimeImmutable('2022-01-04')))
                            )
                        ),
                        new NativeEntryFactory()
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
        $paths = \iterator_to_array((new LocalFilesystem())->scan(new Path(__DIR__ . '/Fixtures/partitioned/**/*.txt'), new NoopFilter()));
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
                (new LocalFilesystem())
                    ->scan(
                        new Path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
                        new ScalarFunctionFilter(ref('partition_01')->equals(lit('b')), new NativeEntryFactory())
                    )
            )
        );
    }

    public function test_reading_partitioned_folder_with_pattern() : void
    {
        $paths = \iterator_to_array((new LocalFilesystem())->scan(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'), new NoopFilter()));
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
        $fs = new LocalFilesystem();

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
        $fs = new LocalFilesystem();

        $stream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        $this->assertTrue($fs->exists($stream->path()));
        $fs->rm($stream->path());
        $this->assertFalse($fs->exists($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = new LocalFilesystem();

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
