<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\Partitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;

use function array_map;
use function file_get_contents;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function sort;

final class OverwriteMultipleFilesTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public function test_multiple_writes_to_same_partition_with_different_filenames(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);

        $sales = $this->context->commit($this->context->path(__FUNCTION__ . '/sales.csv'), overwrite());
        $sales->open([new Partition('partition', 'value')])->append('sales data');
        $sales->commit([0]);

        $orders = $this->context->commit($this->context->path(__FUNCTION__ . '/orders.csv'), overwrite());
        $orders->open([new Partition('partition', 'value')])->append('orders data');
        $orders->commit([0]);

        $files = iterator_to_array($this->fs()->list(path(
            $this->cacheDir->path() . '/' . __FUNCTION__ . '/partition=value/*',
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

    public function test_overwrite_cleans_up_appended_files_of_the_same_name(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [
                    'file-0123456789abcdef.csv' => 'appended file content',
                    'file.csv' => 'original file content',
                ],
            ],
        ]);

        $files = $this->context->commit($this->context->path(__FUNCTION__ . '/file.csv'), overwrite());
        $files->open([new Partition('partition', 'value')])->append('overwritten content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path(
            $this->cacheDir->path() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('overwritten content', file_get_contents($files[0]->path->path()));
    }

    public function test_overwrite_does_not_delete_files_with_different_basename(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [
                    'sales.csv' => 'sales data',
                ],
            ],
        ]);

        $files = $this->context->commit($this->context->path(__FUNCTION__ . '/orders.csv'), overwrite());
        $files->open([new Partition('partition', 'value')])->append('orders data');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path(
            $this->cacheDir->path() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(2, $files);

        $basenames = array_map(static fn($file) => $file->path->basename(), $files);
        sort($basenames);

        static::assertSame(['orders.csv', 'sales.csv'], $basenames);
    }

    public function test_overwrite_replaces_file_with_same_basename(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.csv' => 'old content',
                ],
            ],
        ]);

        $files = $this->context->commit($this->context->path(__FUNCTION__ . '/file.csv'), overwrite());
        $files->open([new Partition('partition', 'value')])->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path(
            $this->cacheDir->path() . '/' . __FUNCTION__ . '/partition=value/*',
        )));

        static::assertCount(1, $files);
        static::assertSame('file.csv', $files[0]->path->basename());
        static::assertSame('new content', file_get_contents($files[0]->path->path()));
    }
}
