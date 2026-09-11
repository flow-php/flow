<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink\Partitioned;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\Integration\Filesystem\FilesSink\FilesSinkTestCase;
use Flow\Filesystem\Partition;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\ignore;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class IgnoreModeTest extends FilesSinkTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_partition_with_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = $this->files($file);

        $files->writeTo([new Partition('partition', 'value')])->append('new content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertStringStartsWith('file.txt', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_existing_partition_without_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = $this->files($file);

        $files->writeTo([new Partition('partition', 'value')])->append('appended content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('appended content', file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_non_existing_partition(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/file.txt');
        $files = $this->files($file);

        $files->writeTo([new Partition('partition', 'value')])->append('appended content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('appended content', file_get_contents($files[0]->path->path()));
    }

    protected function saveMode(): SaveMode
    {
        return ignore();
    }
}
