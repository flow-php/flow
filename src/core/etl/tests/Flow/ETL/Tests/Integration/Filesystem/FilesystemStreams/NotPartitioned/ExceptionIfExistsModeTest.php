<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\NotPartitioned;

use function Flow\ETL\DSL\exception_if_exists;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;

final class ExceptionIfExistsModeTest extends FilesystemStreamsTestCase
{
    protected function tearDown() : void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file() : void
    {
        $this->expectExceptionMessageMatches('/Destination path (.*) already exists, please change path to different or set different SaveMode/');
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/existing-file.txt');

        $streams->writeTo($file);
    }

    public function test_open_stream_for_non_existing_file() : void
    {
        $streams = $this->streams();
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $fileStream = $streams->writeTo($file);
        \fwrite($fileStream->resource(), 'some content');
        $streams->closeWriters($file);

        $this->assertFileExists($file->path());
        $this->assertSame('some content', \file_get_contents($file->path()));
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fs);
        $streams->setSaveMode(exception_if_exists());

        return $streams;
    }
}
