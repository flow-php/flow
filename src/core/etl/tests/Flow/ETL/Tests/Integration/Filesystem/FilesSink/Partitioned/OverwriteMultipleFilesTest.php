<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink\Partitioned;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\Integration\Filesystem\FilesSink\FilesSinkTestCase;
use Flow\Filesystem\Partition;
use Override;

use function array_map;
use function file_get_contents;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function sort;

final class OverwriteMultipleFilesTest extends FilesSinkTestCase
{
    #[Override]
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

        $sales = $this->files($this->getPath(__FUNCTION__ . '/sales.csv'));
        $sales->writeTo([new Partition('partition', 'value')])->append('sales data');
        $sales->publish();

        $orders = $this->files($this->getPath(__FUNCTION__ . '/orders.csv'));
        $orders->writeTo([new Partition('partition', 'value')])->append('orders data');
        $orders->publish();

        $files = iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(2, $files);

        $basenames = array_map(static fn($file) => $file->path->basename(), $files);
        sort($basenames);

        static::assertSame(['orders.csv', 'sales.csv'], $basenames);

        $contentByBasename = [];

        foreach ($files as $file) {
            $contentByBasename[$file->path->basename()] = file_get_contents($file->path->path());
        }

        static::assertSame('sales data', $contentByBasename['sales.csv']);
        static::assertSame('orders data', $contentByBasename['orders.csv']);
    }

    public function test_overwrite_cleans_up_randomized_files_with_same_basename(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file_abc123.csv' => 'randomized file content',
                    'file.csv' => 'original file content',
                ],
            ],
        ]);

        $files = $this->files($this->getPath(__FUNCTION__ . '/file.csv'));
        $files->writeTo([new Partition('partition', 'value')])->append('overwritten content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('overwritten content', file_get_contents($files[0]->path->path()));
    }

    public function test_overwrite_does_not_delete_files_with_different_basename(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'sales.csv' => 'sales data',
                ],
            ],
        ]);

        $files = $this->files($this->getPath(__FUNCTION__ . '/orders.csv'));
        $files->writeTo([new Partition('partition', 'value')])->append('orders data');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(2, $files);

        $basenames = array_map(static fn($file) => $file->path->basename(), $files);
        sort($basenames);

        static::assertSame(['orders.csv', 'sales.csv'], $basenames);
    }

    public function test_overwrite_replaces_file_with_same_basename(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.csv' => 'old content',
                ],
            ],
        ]);

        $files = $this->files($this->getPath(__FUNCTION__ . '/file.csv'));
        $files->writeTo([new Partition('partition', 'value')])->append('new content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path(
            $this->filesDirectory() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('new content', file_get_contents($files[0]->path->path()));
    }

    protected function saveMode(): SaveMode
    {
        return overwrite();
    }
}
