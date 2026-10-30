<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader\File\FileCommit\NotPartitioned;

use Flow\ETL\Tests\Context\FileCommitContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function file_get_contents;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class OverwriteModeTest extends FlowIntegrationTestCase
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
        $files = $this->context->commit($path, overwrite());

        $fileStream = $files->open();
        static::assertStringContainsString('.flow-tmp-', $fileStream->path()->path());
        $fileStream->append('some other content');
        static::assertSame('some content', file_get_contents($path->path()));

        $files->commit([0]);

        static::assertSame('some other content', file_get_contents($path->path()));

        static::assertCount(
            1,
            $files = iterator_to_array($this->fs()->list(path($path->parentDirectory()->path() . '/*'))),
        );
        static::assertSame('existing-file.txt', $files[0]->path->basename());
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->context->files([__FUNCTION__ => []]);
        $path = $this->context->path(__FUNCTION__ . '/non-existing-file.txt');

        $files = $this->context->commit($path, overwrite());
        $files->open()->append('some content');
        $files->commit([0]);

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }
}
