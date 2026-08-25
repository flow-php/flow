<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink\NotPartitioned;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\Integration\Filesystem\FilesSink\FilesSinkTestCase;
use Override;

use function file_get_contents;
use function Flow\ETL\DSL\ignore;

final class IgnoreModeTest extends FilesSinkTestCase
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
        $path = $this->getPath(__FUNCTION__ . '/existing-file.txt');

        $files = $this->files($path);
        $files->writeTo()->append('different content');
        $files->publish();

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }

    public function test_open_stream_for_non_existing_file(): void
    {
        $this->setupFiles([
            __FUNCTION__ => [],
        ]);
        $path = $this->getPath(__FUNCTION__ . '/non-existing-file.txt');

        $files = $this->files($path);
        $files->writeTo()->append('some content');
        $files->publish();

        static::assertFileExists($path->path());
        static::assertSame('some content', file_get_contents($path->path()));
    }

    protected function saveMode(): SaveMode
    {
        return ignore();
    }
}
