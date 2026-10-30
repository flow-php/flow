<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\Partitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;

use function file_get_contents;
use function Flow\ETL\DSL\append;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AppendModeTest extends FlowIntegrationTestCase
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
        $files = $this->context->commit($file, append());

        $files->open([new Partition('partition', 'value')])->append('appended content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/**/*.txt')));

        static::assertCount(2, $files);

        foreach ($files as $streamFile) {
            static::assertStringStartsWith('file', $streamFile->path->basename());
            static::assertStringEndsWith('.txt', $streamFile->path->basename());
        }
    }

    public function test_open_stream_for_existing_partition_without_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'partition=value' => [],
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/file.txt');
        $files = $this->context->commit($file, append());

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
        $files = $this->context->commit($file, append());

        $files->open([new Partition('partition', 'value')])->append('appended content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/partition=value/*')));

        static::assertCount(1, $files);

        static::assertSame('file.txt', $files[0]->path->basename());
        static::assertSame('appended content', file_get_contents($files[0]->path->path()));
    }
}
