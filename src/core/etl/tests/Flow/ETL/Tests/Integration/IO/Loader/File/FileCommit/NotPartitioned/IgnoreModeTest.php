<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\NotPartitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function file_get_contents;
use function Flow\ETL\DSL\ignore;

final class IgnoreModeTest extends FlowIntegrationTestCase
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
        $path = $this->context->path(__FUNCTION__ . '/existing-file.txt');

        $files = $this->context->commit($path, ignore());

        static::assertFalse($files->admit(false));

        $files->commit([0]);

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->context->files([
            __FUNCTION__ => [],
        ]);
        $path = $this->context->path(__FUNCTION__ . '/non-existing-file.txt');

        $files = $this->context->commit($path, ignore());
        $files->open()->append('some content');
        $files->commit([0]);

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }
}
