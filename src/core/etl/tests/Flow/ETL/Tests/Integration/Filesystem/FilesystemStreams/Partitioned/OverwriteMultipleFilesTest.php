<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\Partitioned;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams\FilesystemStreamsTestCase;
use Flow\Filesystem\Partition;

use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;

final class OverwriteMultipleFilesTest extends FilesystemStreamsTestCase
{
    #[\Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_multiple_writes_to_same_partition_with_different_filenames(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);

        $salesStreams = $this->streams();
        $salesFile = $this->getPath(__FUNCTION__ . '/sales.csv');
        $salesStream = $salesStreams->writeTo($salesFile, partitions: [new Partition('partition', 'value')]);
        $salesStream->append('sales data');
        $salesStreams->closeStreams($salesFile);

        $ordersStreams = $this->streams();
        $ordersFile = $this->getPath(__FUNCTION__ . '/orders.csv');
        $ordersStream = $ordersStreams->writeTo($ordersFile, partitions: [new Partition('partition', 'value')]);
        $ordersStream->append('orders data');
        $ordersStreams->closeStreams($ordersFile);

        $files = \iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(2, $files);

        $basenames = \array_map(static fn($file) => $file->path->basename(), $files);
        \sort($basenames);

        static::assertSame(['orders.csv', 'sales.csv'], $basenames);

        $contentByBasename = [];

        foreach ($files as $file) {
            $contentByBasename[$file->path->basename()] = \file_get_contents($file->path->path());
        }

        static::assertSame('sales data', $contentByBasename['sales.csv']);
        static::assertSame('orders data', $contentByBasename['orders.csv']);
    }

    public function test_overwrite_cleans_up_randomized_files_with_same_basename(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file_abc123.csv' => 'randomized file content',
                    'file.csv' => 'original file content',
                ],
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/file.csv');
        $fileStream = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $fileStream->append('overwritten content');
        $streams->closeStreams($file);

        $files = \iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('overwritten content', \file_get_contents($files[0]->path->path()));
    }

    public function test_overwrite_does_not_delete_files_with_different_basename(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'sales.csv' => 'sales data',
                ],
            ],
        ]);

        $ordersFile = $this->getPath(__FUNCTION__ . '/orders.csv');
        $ordersStream = $streams->writeTo($ordersFile, partitions: [new Partition('partition', 'value')]);
        $ordersStream->append('orders data');
        $streams->closeStreams($ordersFile);

        $files = \iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(2, $files);

        $basenames = \array_map(static fn($file) => $file->path->basename(), $files);
        \sort($basenames);

        static::assertSame(['orders.csv', 'sales.csv'], $basenames);
    }

    public function test_overwrite_replaces_file_with_same_basename(): void
    {
        $streams = $this->streams();

        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.csv' => 'old content',
                ],
            ],
        ]);

        $file = $this->getPath(__FUNCTION__ . '/file.csv');
        $fileStream = $streams->writeTo($file, partitions: [new Partition('partition', 'value')]);
        $fileStream->append('new content');
        $streams->closeStreams($file);

        $files = \iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('new content', \file_get_contents($files[0]->path->path()));
    }

    protected function streams(): FilesystemStreams
    {
        $streams = new FilesystemStreams($this->fstab());
        $streams->setMode(overwrite());

        return $streams;
    }
}
