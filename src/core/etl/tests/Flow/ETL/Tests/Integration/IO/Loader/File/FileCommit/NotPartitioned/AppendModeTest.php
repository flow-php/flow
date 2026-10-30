<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\NotPartitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;

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

    public function test_open_stream_for_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/existing-file.txt');
        static::assertFileExists($file->path());

        $files = $this->context->commit($file, append());
        $files->open()->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*')));

        static::assertCount(2, $files);

        foreach ($files as $streamFile) {
            static::assertStringStartsWith('existing-file', $streamFile->path->basename());
            static::assertStringEndsWith('.txt', $streamFile->path->basename());
        }
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/non-existing-file.txt');
        static::assertFileDoesNotExist($file->path());

        $files = $this->context->commit($file, append());
        $files->open()->append('new content');
        $files->commit([0]);

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*')));

        static::assertCount(1, $files);
        static::assertSame('non-existing-file.txt', $files[0]->path->basename());
    }
}
