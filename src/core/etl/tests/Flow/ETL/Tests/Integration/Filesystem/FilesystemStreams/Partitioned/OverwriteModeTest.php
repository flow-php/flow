<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\Partitioned;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Tests\Double\FakeNativeLocalFilesystem;

use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;

final class OverwriteModeTest extends FilesystemStreamsTestCase
{
    #[\Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_partition_with_existing_file(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $fileStream->append('new content');
        $streams->closeStreams($file);

        $files = \iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertStringStartsWith('file.txt', $files[0]->path->basename());
        static::assertSame('new content', \file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_existing_partition_without_existing_file(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $fileStream->append('new content');
        $streams->closeStreams($file);

        $files = \iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('new content', \file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_non_existing_partition(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');

        $appendedFile = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $appendedFile->append('new content');
        $streams->closeStreams($file);
        $files = \iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('new content', \file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_non_existing_partition_with_custom_schema(): void
    {
        $this->setupFiles([__FUNCTION__ => []]);

        $fs = new FakeNativeLocalFilesystem();

        $file = path(
            $fs->mount()->protocol . '://' . $this->filesDirectory() . DIRECTORY_SEPARATOR . __FUNCTION__ . '/file.txt',
        );

        $streams = new FilesystemStreams(new FilesystemTable($fs));
        $streams->setMode(overwrite());

        $appendedFile = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $appendedFile->append('new content');

        $streams->closeStreams($file);
        $files = \iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('new content', \file_get_contents($files[0]->path->path()));
    }

    protected function streams(): FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(overwrite());

        return $streams;
    }
}
