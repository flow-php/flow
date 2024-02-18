<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\append;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;

final class AppendModeTest extends FilesystemStreamsTestCase
{
    protected function tearDown() : void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/existing-file.txt');
        $this->assertFileExists($file->path());

        $appendFileStream = $streams->writeTo($file);
        \fwrite($appendFileStream->resource(), 'new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/*')));

        $this->assertCount(2, $files);

        foreach ($files as $streamFile) {
            $this->assertStringStartsWith('existing-file', $streamFile->basename());
            $this->assertStringEndsWith('.txt', $streamFile->basename());
        }
    }

    public function test_open_stream_for_non_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');
        $this->assertFileDoesNotExist($file->path());

        $appendFileStream = $streams->writeTo($file);
        \fwrite($appendFileStream->resource(), 'new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/*')));

        $this->assertCount(1, $files);
        $this->assertSame('non-existing-file.txt', $files[0]->basename());
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fs);
        $streams->setSaveMode(append());

        return $streams;
    }
}
