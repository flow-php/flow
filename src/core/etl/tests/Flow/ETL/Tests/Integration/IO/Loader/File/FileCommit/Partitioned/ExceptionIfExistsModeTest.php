<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\Partitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;

use function file_get_contents;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ExceptionIfExistsModeTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public function test_open_stream_for_existing_partition_with_existing_file(): void
    {
        $this->expectExceptionMessageMatches(
            '/Destination path (.*) already exists, please change path to different or set different SaveMode/',
        );

        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [
                    'file.txt' => 'file content',
                ],
            ],
        ]);

        $this->context->commit($this->context->path(__FUNCTION__ . '/file.txt'), exception_if_exists())->admit(true);
    }

    public function test_open_stream_for_existing_partition_without_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, exception_if_exists());

        $files->open([new Partition('partition', 'value')])->append('file content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }

    public function test_open_stream_for_non_existing_partition(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, exception_if_exists());

        $files->open([new Partition('partition', 'value')])->append('file content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('file content', file_get_contents($files[0]->path->path()));
    }
}
