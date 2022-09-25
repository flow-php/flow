<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\FlysystemFS;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

class FilesystemStreamsTest extends TestCase
{
    public function test_closing_stream_with_non_thread_safe_base_path() : void
    {
        $streams = (new FilesystemStreams(new FlysystemFS()));

        $streams->open($csvPath = Path::realpath(\sys_get_temp_dir() . '/file.csv'), 'csv', Mode::APPEND, false);
        $streams->open($jsonPath = Path::realpath(\sys_get_temp_dir() . '/file.json'), 'json', Mode::APPEND, false);

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

        $stream = (new FilesystemStreams(new FlysystemFS()))
            ->open(Path::realpath($dir = \sys_get_temp_dir()), 'csv', Mode::APPEND, false, $rows->partitions)
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

        $stream = (new FilesystemStreams(new FlysystemFS()))
            ->open(Path::realpath($dir = \sys_get_temp_dir() . '/file.csv'), 'csv', Mode::APPEND, false)
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
}
