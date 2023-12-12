<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Filesystem\Stream\VoidStreamWrapper;
use Flow\ETL\Partition;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

class FilesystemStreamsTest extends IntegrationTestCase
{
    public function test_closing_stream_with_non_append_safe_base_path() : void
    {
        $streams = (new FilesystemStreams(new LocalFilesystem()));

        if (\file_exists(\sys_get_temp_dir() . '/file.json')) {
            \unlink(\sys_get_temp_dir() . '/file.json');
        }

        if (\file_exists(\sys_get_temp_dir() . '/file.csv')) {
            \unlink(\sys_get_temp_dir() . '/file.csv');
        }

        $streams->open($csvPath = Path::realpath(\sys_get_temp_dir() . '/file.csv'), 'csv', false);
        $streams->open($jsonPath = Path::realpath(\sys_get_temp_dir() . '/file.json'), 'json', false);

        $streams->close($jsonPath);

        $this->assertEquals(
            $csvPath,
            \current(\iterator_to_array($streams))->path()
        );
    }

    public function test_open_partitioned_rows() : void
    {
        $rows = (new Rows(...[
            Row::create(int_entry('id', 1), str_entry('group', 'a')),
            Row::create(int_entry('id', 2), str_entry('group', 'a')),
            Row::create(int_entry('id', 3), str_entry('group', 'b')),
            Row::create(int_entry('id', 4), str_entry('group', 'b')),
            Row::create(int_entry('id', 5), str_entry('group', 'b')),
        ]))->partitionBy('group')[0];

        $stream = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Overwrite)
            ->open(Path::realpath($dir = \rtrim(\sys_get_temp_dir(), '/')), 'csv', false, $rows->partitions()->toArray())
            ->path();
        $this->assertStringStartsWith(
            $dir . '/group=a/',
            $stream->path()
        );
        $this->assertStringEndsWith(
            '.csv',
            $stream->path()
        );

        if (\file_exists($stream->path())) {
            \unlink($stream->path());
        }
    }

    public function test_open_rows() : void
    {
        (new Rows(...[
            Row::create(int_entry('id', 1), str_entry('group', 'a')),
            Row::create(int_entry('id', 2), str_entry('group', 'a')),
            Row::create(int_entry('id', 3), str_entry('group', 'b')),
            Row::create(int_entry('id', 4), str_entry('group', 'b')),
            Row::create(int_entry('id', 5), str_entry('group', 'b')),
        ]));

        $stream = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Overwrite)
            ->open(Path::realpath($dir = \rtrim(\sys_get_temp_dir(), '/') . '/file.csv'), 'csv', false)
            ->path();
        $this->assertStringStartsWith(
            $dir,
            $stream->path()
        );
        $this->assertStringEndsWith(
            '.csv',
            $stream->path()
        );

        if (\file_exists($stream->path())) {
            \unlink($stream->path());
        }
    }

    public function test_open_stream_to_existing_file_in_non_append_safe_and_append_mode() : void
    {
        $path = Path::realpath(__DIR__ . '/Fixtures/file.txt');
        $streams = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Append);

        $this->assertFalse($streams->isOpen($path));

        $this->expectExceptionMessage("Appending to destination \"{$path->uri()}\" in non append safe mode is not supported");
        $streams->open($path, 'txt', false);
    }

    public function test_open_stream_to_existing_file_in_non_append_safe_and_exception_when_exists_mode() : void
    {
        $path = Path::realpath(__DIR__ . '/Fixtures/file.txt');
        $this->expectExceptionMessage("Destination path \"{$path->uri()}\" already exists, please change path to different or set different SaveMode");
        $streams = (new FilesystemStreams(new LocalFilesystem()));

        $this->assertFalse($streams->isOpen($path));

        $streams->open($path, 'txt', false);
    }

    public function test_open_stream_to_existing_file_in_non_append_safe_and_ignore_mode() : void
    {
        $path = Path::realpath(__DIR__ . '/Fixtures/file.txt');
        $streams = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Ignore);

        $this->assertFalse($streams->isOpen($path));

        $stream = $streams->open($path, 'txt', false);

        $this->assertInstanceOf(
            VoidStreamWrapper::class,
            \stream_get_meta_data($stream->resource())['wrapper_data']
        );
        $this->assertCount(1, $streams);
    }

    public function test_open_stream_to_existing_file_in_non_append_safe_and_overwrite_mode() : void
    {
        $path = Path::tmpFile('txt');

        if (\file_exists($path->path())) {
            \unlink($path->path());
        }

        \file_put_contents($path->path(), 'test');

        $streams = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Overwrite);

        $this->assertFalse($streams->isOpen($path));

        $stream = $streams->open($path, 'txt', false);

        $this->assertSame(
            '', // overwrite mode deleted original file with content 'test' and created a new empty file
            \file_get_contents($stream->path()->path())
        );
    }

    public function test_opening_multiple_non_existing_partitions() : void
    {
        $basePath = Path::realpath(\sys_get_temp_dir() . '/filesystem-streams-test/');

        if ($this->fs->directoryExists($basePath)) {
            $this->fs->rm($basePath);
        }

        \mkdir($basePath->path(), recursive: true);

        $streams = (new FilesystemStreams(new LocalFilesystem()));
        $partition1Stream = $streams->open($basePath, 'txt', true, [new Partition('partition', '1')]);
        $this->assertTrue($streams->isOpen($basePath, [new Partition('partition', '1')]));
        $this->assertStringEndsWith(
            '.txt',
            $partition1Stream->path()->path()
        );
        $this->assertFileExists($partition1Stream->path()->path());

        $streams->close($basePath);
        $this->assertFalse($streams->isOpen($basePath, [new Partition('partition', '1')]));
    }

    public function test_opening_multiple_partitions_in_overwrite_mode() : void
    {
        $basePath = Path::realpath(\sys_get_temp_dir() . '/filesystem-streams-test/');
        $partition1Path = $basePath->addPartitions(new Partition('partition', '1'));
        $partition2Path = $basePath->addPartitions(new Partition('partition', '2'));

        if ($this->fs->directoryExists($basePath)) {
            $this->fs->rm($basePath);
        }

        \mkdir($basePath->path(), recursive: true);
        \mkdir($partition1Path->path(), recursive: true);
        \mkdir($partition2Path->path(), recursive: true);

        \file_put_contents($partition1Path->path() . '/file.txt', 'partition_1');
        \file_put_contents($partition2Path->path() . '/file.txt', 'partition_2');

        $streams = (new FilesystemStreams(new LocalFilesystem()))
            ->setSaveMode(SaveMode::Overwrite);

        $partition1Stream = $streams->open($basePath, 'txt', true, [new Partition('partition', '1')]);

        $this->assertTrue($streams->isOpen($basePath, [new Partition('partition', '1')]));
        $this->assertFileDoesNotExist($partition1Path->path() . '/file.txt');
        $this->assertSame('', \file_get_contents($partition1Stream->path()->path()));
        $this->assertSame('partition_2', \file_get_contents($partition2Path->path() . '/file.txt'));
        $this->assertStringEndsWith(
            '.txt',
            $partition1Stream->path()->path()
        );
        $this->assertFileExists($partition1Stream->path()->path());

        $streams->close($basePath);
        $this->assertFalse($streams->isOpen($basePath, [new Partition('partition', '1')]));

        $streams->rm($basePath);
        $this->assertFileDoesNotExist($basePath->path());
        $this->assertFileDoesNotExist($partition1Path->path() . '/file.txt');
        $this->assertFileDoesNotExist($partition2Path->path() . '/file.txt');
    }

    public function test_overwrite_mode_on_processed_partitions() : void
    {
        $partitionedRows = (new Rows(...[
            Row::create(int_entry('id', 1), str_entry('group', 'a')),
            Row::create(int_entry('id', 2), str_entry('group', 'a')),
            Row::create(int_entry('id', 3), str_entry('group', 'b')),
            Row::create(int_entry('id', 4), str_entry('group', 'b')),
            Row::create(int_entry('id', 5), str_entry('group', 'b')),
        ]))->partitionBy('group');

        $streams = new FilesystemStreams(new LocalFilesystem());
        $streams->setSaveMode(SaveMode::Overwrite);

        $groupAStream = $streams
            ->open(Path::realpath($dir = \rtrim(\sys_get_temp_dir(), '/')), 'csv', false, $partitionedRows[0]->partitions()->toArray())
            ->path();
        $this->assertStringStartsWith(
            $dir . '/group=a/',
            $groupAStream->path()
        );
        $groupBStream = $streams
            ->open(Path::realpath($dir), 'csv', false, $partitionedRows[1]->partitions()->toArray())
            ->path();
        $this->assertStringStartsWith(
            $dir . '/group=b/',
            $groupBStream->path()
        );
        \file_put_contents($groupAStream->path(), 'test_a');
        \file_put_contents($groupBStream->path(), 'test_b');

        // Close all streams
        $streams->close(Path::realpath($dir));

        // Open stream again, but just touched partition
        $groupAStream = $streams
            ->open(Path::realpath($dir), 'csv', false, $partitionedRows[0]->partitions()->toArray())
            ->path();

        $this->assertEquals('', \file_get_contents($groupAStream->path()));
        $this->assertEquals('test_b', \file_get_contents($groupBStream->path()));
    }
}
