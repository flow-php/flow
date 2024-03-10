<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem\Tests\Integration;

use function Flow\ETL\DSL\{all, lit, ref};
use Flow\ETL\Adapter\Filesystem\FlysystemFS;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\{NoopFilter, ScalarFunctionFilter};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class FlysystemFSTest extends TestCase
{
    public function test_append_mode() : void
    {
        $fs = new FlysystemFS();

        $stream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/append.txt'), Mode::APPEND_WRITE);
        \fwrite($stream->resource(), "some data to make file not empty\n");
        $stream->close();

        $appendStream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/append.txt'), Mode::APPEND_WRITE);
        \fwrite($appendStream->resource(), "some more data to make file not empty\n");
        $appendStream->close();

        self::assertStringContainsString(
            <<<'STRING'
some data to make file not empty
some more data to make file not empty
STRING,
            \file_get_contents($appendStream->path()->path())
        );

        $fs->rm($stream->path());
        self::assertFalse($fs->exists($stream->path()));
    }

    public function test_dir_exists() : void
    {
        self::assertTrue((new FlysystemFS())->exists(new Path(__DIR__)));
        self::assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists() : void
    {
        self::assertTrue((new FlysystemFS())->exists(new Path(__FILE__)));
        self::assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists() : void
    {
        self::assertTrue((new FlysystemFS())->exists(new Path(__DIR__ . '/**/*.txt')));
        self::assertFalse((new FlysystemFS())->exists(new Path(__DIR__ . '/**/*.pdf')));
    }

    public function test_open_file_stream_for_existing_file() : void
    {
        $stream = (new FlysystemFS())->open(new Path(__FILE__), Mode::READ);

        self::assertIsResource($stream->resource());
        self::assertSame(
            \file_get_contents(__FILE__),
            \stream_get_contents($stream->resource())
        );
    }

    public function test_open_file_stream_for_non_existing_file() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_test_file_', true) . '.txt';

        $stream = (new FlysystemFS())->open(new Path($path), Mode::WRITE);

        self::assertIsResource($stream->resource());
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $paths = \iterator_to_array(
            (new FlysystemFS())
                ->scan(
                    new Path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                    new ScalarFunctionFilter(
                        all(
                            ref('country')->equals(lit('pl')),
                            all(
                                ref('date')->cast('date')->greaterThanEqual(lit(new \DateTimeImmutable('2022-01-02 00:00:00'))),
                                ref('date')->cast('date')->lessThan(lit(new \DateTimeImmutable('2022-01-04 00:00:00')))
                            )
                        ),
                        new NativeEntryFactory()
                    )
                )
        );
        \sort($paths);

        self::assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt'),
                new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt'),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder() : void
    {
        $paths = \iterator_to_array((new FlysystemFS())->scan(new Path(__DIR__ . '/Fixtures/partitioned**/*.txt'), new NoopFilter()));
        \sort($paths);

        self::assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'),
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder_with_partitions_filtering() : void
    {
        self::assertEquals(
            [
                new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'),
            ],
            \iterator_to_array(
                (new FlysystemFS())
                    ->scan(
                        new Path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
                        new ScalarFunctionFilter(ref('partition_01')->equals(lit('b')), new NativeEntryFactory())
                    )
            )
        );
    }

    public function test_reading_partitioned_folder_with_pattern() : void
    {
        $paths = \iterator_to_array((new FlysystemFS())->scan(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'), new NoopFilter()));
        \sort($paths);

        self::assertEquals(
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

        self::assertTrue($fs->exists($dirPath));
        self::assertTrue($fs->exists($stream->path()));
        $fs->rm($dirPath);
        self::assertFalse($fs->exists($dirPath));
        self::assertFalse($fs->exists($stream->path()));
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = new FlysystemFS();

        $stream = $fs->open(Path::realpath(\sys_get_temp_dir() . '/flow-fs-test/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->exists($stream->path()));
        $fs->rm($stream->path());
        self::assertFalse($fs->exists($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = new FlysystemFS();

        $dirPath = Path::realpath(\sys_get_temp_dir() . '/flow-fs-test-directory/');

        $stream = $fs->open(Path::realpath($dirPath->path() . '/remove_file_when_exists.txt'), Mode::WRITE);
        \fwrite($stream->resource(), 'some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->exists($dirPath));
        self::assertTrue($fs->exists($stream->path()));
        $fs->rm(Path::realpath($dirPath->path() . '/*.txt'));
        self::assertTrue($fs->exists($dirPath));
        self::assertFalse($fs->exists($stream->path()));
        $fs->rm($dirPath);
    }
}
