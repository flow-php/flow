<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\NotPartitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function file_get_contents;
use function Flow\ETL\DSL\exception_if_exists;

final class ExceptionIfExistsModeTest extends FlowIntegrationTestCase
{
    public FileCommitContext $context;

    protected function setUp(): void
    {
        parent::setUp();
        $this->context = new FileCommitContext($this->cacheDir, $this->fs());
    }

    public function test_open_stream_for_existing_file(): void
    {
        $this->expectExceptionMessageMatches(
            '/Destination path (.*) already exists, please change path to different or set different SaveMode/',
        );

        $this->context->files([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);

        $this->context
            ->commit($this->context->path(__FUNCTION__ . '/existing-file.txt'), exception_if_exists())
            ->admit(false);
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);
        $file = $this->context->path(__FUNCTION__ . '/non-existing-file.txt');

        $files = $this->context->commit($file, exception_if_exists());
        $files->open()->append('some content');
        $files->commit([0]);

        static::assertFileExists($file->path());
        static::assertSame('some content', file_get_contents($file->path()));
    }
}
