<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\append;
use Flow\ETL\Filesystem\{FilesystemStreams};
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Flow\Filesystem\Path;

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
        $appendFileStream->append('new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs()->list(new Path($file->parentDirectory()->path() . '/*')));

        self::assertCount(2, $files);

        foreach ($files as $streamFile) {
            self::assertStringStartsWith('existing-file', $streamFile->path->basename());
            self::assertStringEndsWith('.txt', $streamFile->path->basename());
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
        $appendFileStream->append('new content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs()->list(new Path($file->parentDirectory()->path() . '/*')));

        self::assertCount(1, $files);
        self::assertSame('non-existing-file.txt', $files[0]->path->basename());
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setSaveMode(append());

        return $streams;
    }
}
