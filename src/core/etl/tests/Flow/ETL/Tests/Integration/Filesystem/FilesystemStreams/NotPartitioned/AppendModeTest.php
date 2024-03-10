<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\append;
use Flow\ETL\Filesystem\{FilesystemStreams, Path};
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
        self::assertFileExists($file->path());

        $appendFileStream = $streams->writeTo($file);
        \fwrite($appendFileStream->resource(), 'new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/*')));

        self::assertCount(2, $files);

        foreach ($files as $streamFile) {
            self::assertStringStartsWith('existing-file', $streamFile->basename());
            self::assertStringEndsWith('.txt', $streamFile->basename());
        }
    }

    public function test_open_stream_for_non_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');
        self::assertFileDoesNotExist($file->path());

        $appendFileStream = $streams->writeTo($file);
        \fwrite($appendFileStream->resource(), 'new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/*')));

        self::assertCount(1, $files);
        self::assertSame('non-existing-file.txt', $files[0]->basename());
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fs);
        $streams->setSaveMode(append());

        return $streams;
    }
}
