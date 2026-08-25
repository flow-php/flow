<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink\NotPartitioned;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\Integration\Filesystem\FilesSink\FilesSinkTestCase;
use Override;

use function Flow\ETL\DSL\append;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AppendModeTest extends FilesSinkTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanFiles();
    }

    public function test_open_stream_for_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [
                'existing-file.txt' => 'some content',
            ],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/existing-file.txt');
        static::assertFileExists($file->path());

        $files = $this->files($file);
        $files->writeTo()->append('new content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*')));

        static::assertCount(2, $files);

        foreach ($files as $streamFile) {
            static::assertStringStartsWith('existing-file', $streamFile->path->basename());
            static::assertStringEndsWith('.txt', $streamFile->path->basename());
        }
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $file = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');
        static::assertFileDoesNotExist($file->path());

        $files = $this->files($file);
        $files->writeTo()->append('new content');
        $files->publish();

        $files = iterator_to_array($this->fs()->list(path($file->parentDirectory()->path() . '/*')));

        static::assertCount(1, $files);
        static::assertSame('non-existing-file.txt', $files[0]->path->basename());
    }

    protected function saveMode(): SaveMode
    {
        return append();
    }
}
