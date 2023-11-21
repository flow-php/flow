<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

class FilesystemStreamsTest extends TestCase
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
            Row::create(Entry::integer('id', 1), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 2), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 3), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 4), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 5), Entry::string('group', 'b')),
        ]))->partitionBy('group')[0];

        $stream = (new FilesystemStreams(new LocalFilesystem()))
            ->open(Path::realpath($dir = \rtrim(\sys_get_temp_dir(), '/')), 'csv', false, $rows->partitions())
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
            Row::create(Entry::integer('id', 1), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 2), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 3), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 4), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 5), Entry::string('group', 'b')),
        ]));

        $stream = (new FilesystemStreams(new LocalFilesystem()))
            ->setMode(SaveMode::Overwrite)
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

    public function test_overwrite_mode_on_processed_partitions() : void
    {
        $partitionedRows = (new Rows(...[
            Row::create(Entry::integer('id', 1), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 2), Entry::string('group', 'a')),
            Row::create(Entry::integer('id', 3), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 4), Entry::string('group', 'b')),
            Row::create(Entry::integer('id', 5), Entry::string('group', 'b')),
        ]))->partitionBy('group');

        $streams = new FilesystemStreams(new LocalFilesystem());
        $streams->setMode(SaveMode::Overwrite);

        $groupAStream = $streams
            ->open(Path::realpath($dir = \rtrim(\sys_get_temp_dir(), '/')), 'csv', false, $partitionedRows[0]->partitions())
            ->path();
        $this->assertStringStartsWith(
            $dir . '/group=a/',
            $groupAStream->path()
        );
        $groupBStream = $streams
            ->open(Path::realpath($dir), 'csv', false, $partitionedRows[1]->partitions())
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
            ->open(Path::realpath($dir), 'csv', false, $partitionedRows[0]->partitions())
            ->path();

        $this->assertEquals('', \file_get_contents($groupAStream->path()));
        $this->assertEquals('test_b', \file_get_contents($groupBStream->path()));
    }
}
