<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\Partitioned;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class PartitionPlaceholdersTest extends FilesystemStreamsTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_append_mode_randomizes_existing_placeholder_file(): void
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(append());

        $this->setupFiles([
            __FUNCTION__ => [
                '123456-PL.csv' => 'existing content',
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/{order-name}.csv');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('order-name', '123456-PL')]);
        $fileStream->append('new content');
        $streams->closeStreams($file);

        $files = iterator_to_array($this->fs()->list(path($this->getPath(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(2, $files);

        foreach ($files as $streamFile) {
            static::assertStringStartsWith('123456-PL', $streamFile->path->basename());
            static::assertStringEndsWith('.csv', $streamFile->path->basename());
        }
    }

    public function test_list_attaches_partitions_from_placeholders(): void
    {
        $streams = new FilesystemStreams($this->fstab());

        $this->setupFiles([
            __FUNCTION__ => [
                'order-year=2024' => [
                    '123456-PL.csv' => 'file content',
                ],
            ],
        ]);

        $streamsList = iterator_to_array($streams->list(
            $this->getPath(__FUNCTION__ . '/order-year=*/{order-name}.csv'),
            new OnlyFiles(),
        ));

        static::assertCount(1, $streamsList);

        $partitions = $streamsList[0]->path()->partitions();

        static::assertCount(2, $partitions);
        static::assertSame('2024', $partitions->get('order-year')->value);
        static::assertSame('123456-PL', $partitions->get('order-name')->value);
    }

    public function test_overwrite_mode_replaces_existing_placeholder_file(): void
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(overwrite());

        $this->setupFiles([
            __FUNCTION__ => [
                '123456-PL.csv' => 'existing content',
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/{order-name}.csv');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('order-name', '123456-PL')]);
        $fileStream->append('new content');
        $streams->closeStreams($file);

        $files = iterator_to_array($this->fs()->list(path($this->getPath(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertSame('new content', file_get_contents($files[0]->path->path()));
    }

    public function test_write_to_placeholder_path_consuming_all_partitions(): void
    {
        $streams = new FilesystemStreams($this->fstab());

        $this->setupFiles([__FUNCTION__ => []]);
        $file = $this->getPath(__FUNCTION__ . '/{order-name}.csv');

        $fileStream = $streams->writeTo($file, partitions: [new Partition('order-name', '123456-PL')]);
        $fileStream->append('file content');
        $streams->closeStreams($file);

        $files = iterator_to_array($this->fs()->list(path($this->getPath(__FUNCTION__)->path() . '/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }

    public function test_write_to_placeholder_path_with_remaining_partitions(): void
    {
        $streams = new FilesystemStreams($this->fstab());

        $this->setupFiles([__FUNCTION__ => []]);
        $file = $this->getPath(__FUNCTION__ . '/{order-name}.csv');

        $fileStream = $streams->writeTo($file, partitions: [
            new Partition('order-year', '2024'),
            new Partition('order-name', '123456-PL'),
        ]);
        $fileStream->append('file content');
        $streams->closeStreams($file);

        $files = iterator_to_array($this->fs()->list(path($this->getPath(__FUNCTION__)->path() . '/**/*.csv')));

        static::assertCount(1, $files);
        static::assertSame('123456-PL.csv', $files[0]->path->basename());
        static::assertStringContainsString('order-year=2024', $files[0]->path->path());
    }

    public function test_write_to_placeholder_path_without_partitions(): void
    {
        $streams = new FilesystemStreams($this->fstab());

        $this->setupFiles([__FUNCTION__ => []]);
        $file = $this->getPath(__FUNCTION__ . '/{order-name}.csv');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('contains partition placeholders but rows are not partitioned');

        $streams->writeTo($file);
    }

    protected function streams(): FilesystemStreams
    {
        return new FilesystemStreams($this->fstab());
    }
}
