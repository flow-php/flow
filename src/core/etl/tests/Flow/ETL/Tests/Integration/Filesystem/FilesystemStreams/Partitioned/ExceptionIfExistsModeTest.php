<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\Partitioned;

use function Flow\ETL\DSL\exception_if_exists;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Partition;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;

final class ExceptionIfExistsModeTest extends FilesystemStreamsTestCase
{
    protected function tearDown() : void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_partition_with_existing_file() : void
    {
        $this->expectExceptionMessageMatches('/Destination path (.*) already exists, please change path to different or set different SaveMode/');
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
    }

    public function test_open_stream_for_existing_partition_without_existing_file() : void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        \fwrite($fileStream->resource(), 'file content');
        $streams->closeWriters($file);

        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/**/*.txt')));

        $this->assertCount(1, $files);

        $this->assertSame('file.txt', $files[0]->basename());
        $this->assertSame('file content', \file_get_contents($files[0]->path()));
    }

    public function test_open_stream_for_non_existing_partition() : void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $appendedFile = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        \fwrite($appendedFile->resource(), 'file content');
        $streams->closeWriters($file);
        $files = \iterator_to_array($this->fs->scan(new Path($file->parentDirectory()->path() . '/partition=value/*')));

        $this->assertCount(1, $files);

        $this->assertSame('file.txt', $files[0]->basename());
        $this->assertSame('file content', \file_get_contents($files[0]->path()));
    }

    protected function streams() : FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fs);
        $streams->setSaveMode(exception_if_exists());

        return $streams;
    }
}
