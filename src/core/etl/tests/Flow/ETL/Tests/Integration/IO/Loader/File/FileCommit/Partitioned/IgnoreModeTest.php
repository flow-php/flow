<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\Partitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;

use function file_get_contents;
use function Flow\ETL\DSL\ignore;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class IgnoreModeTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public function test_open_stream_for_existing_partition_with_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, ignore());

        static::assertFalse($files->admit(true));

        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertStringStartsWith('file.txt', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_existing_partition_without_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, ignore());

        $files->open([new Partition('partition', 'value')])->append('appended content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('appended content', file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_non_existing_partition(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, ignore());

        $files->open([new Partition('partition', 'value')])->append('appended content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('appended content', file_get_contents($files[0]->path->path()));
    }

    public function test_an_existing_file_in_another_partition_ignores_the_whole_write(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=other' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        // decided once for the destination, as Spark and DuckDB do: no partition is written
        static::assertFalse(
            $this->context->commit($this->context->path(__FUNCTION__ . '/file.txt'), ignore())->admit(true),
        );
    }
}
